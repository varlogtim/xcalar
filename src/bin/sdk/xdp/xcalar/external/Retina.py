# Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json
from json import JSONEncoder

from xcalar.external.LegacyApi.WorkItem import WorkItemExportRetina, WorkItemImportRetina, WorkItemListRetina, WorkItemMakeRetina
from LibApisCommon.ttypes import XcalarApiRetinaDstT, XcalarApiRetinaSrcTableT, XcalarApiParameterT
from DagTypes.ttypes import XcalarApiNamedInputT
from DataTargetTypes.ttypes import ExColumnNameT
from xcalar.external.LegacyApi.WorkItem import WorkItemDeleteRetina
from xcalar.external.LegacyApi.WorkItem import WorkItemGetRetina
from xcalar.external.LegacyApi.WorkItem import WorkItemGetRetinaJson
from xcalar.external.LegacyApi.WorkItem import WorkItemUpdateRetina
from xcalar.external.LegacyApi.Optimizer import Graph
from xcalar.compute.localtypes.Dataflow_pb2 import ExecuteRequest
from xcalar.compute.localtypes.DataflowEnums_pb2 import DFS, BFS


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class Retina(object):
    def __init__(self, xcalarApi):
        self.xcalarApi = xcalarApi

    def list(self):
        workItem = WorkItemListRetina()
        return self.xcalarApi.execute(workItem)

    # The udfUserName and udfSessionName are provided by the caller to specify
    # the source of the UDFs for the retina's UDF references.  This is needed
    # until we have a per-dataflow UDF path.
    def add(self,
            name,
            encodedRetina=None,
            retinaJsonStr=None,
            overwrite=True,
            udfUserName=None,
            udfSessionName=None):
        workItem = WorkItemImportRetina(
            name,
            overwrite,
            encodedRetina,
            retinaJsonStr,
            udfUserName=udfUserName,
            udfSessionName=udfSessionName)
        return self.xcalarApi.execute(workItem)

    def export(self, name):
        workItem = WorkItemExportRetina(name)
        return self.xcalarApi.execute(workItem)

    def make(self, name, dstTableNames, dstColumnNames, srcNames=[]):
        dstTables = []
        srcTables = []

        for table, columns in zip(dstTableNames, dstColumnNames):
            retinaDst = XcalarApiRetinaDstT()
            retinaDst.target = XcalarApiNamedInputT()
            retinaDst.target.name = table
            retinaDst.numColumns = len(columns)
            retinaDst.columns = []

            for columnName in columns:
                # The following check is to allow two signatures for the caller:
                # (A) 'dstColumnNames' -> list of column names (most common)
                # (B) 'dstColumnNames' -> list of (colName, headerAlias) tuples
                # See src/bin/tests/pyTest/test_operators.py for use of (B)
                if isinstance(columnName, tuple):
                    (columnName, headerAlias) = columnName
                else:
                    headerAlias = columnName
                column = ExColumnNameT()
                column.name = columnName
                column.headerAlias = headerAlias
                retinaDst.columns.append(column)

            dstTables.append(retinaDst)

        for srcName in srcNames:
            retinaSrc = XcalarApiRetinaSrcTableT()
            retinaSrc.source = srcName
            retinaSrc.dstName = srcName

            srcTables.append(retinaSrc)

        workItem = WorkItemMakeRetina(name, dstTables, srcTables)
        return self.xcalarApi.execute(workItem)

    def execute(self,
                name,
                params,
                newTableName=None,
                queryName=None,
                schedName=None,
                udfUserName="",
                udfSessionName="",
                parallel_operations=False,
                clean_job_state=True,
                pin_results=False,
                sched_name=None):
        request = ExecuteRequest()
        request.dataflow_name = name
        if (queryName):
            request.job_name = queryName
        request.scope.workbook.name.username = self.xcalarApi.session.username
        request.scope.workbook.name.workbookName = self.xcalarApi.session.name
        request.udf_user_name = udfUserName
        request.udf_session_name = udfSessionName
        request.is_async = False
        if (schedName):
            request.sched_name = schedName
        if (newTableName):
            request.export_to_active_session = True
            request.dest_table = newTableName
        else:
            request.export_to_active_session = False
        request.optimized = True
        request.execution_mode = BFS if parallel_operations is True else DFS
        request.clean_job_state = clean_job_state
        request.pin_results = pin_results
        if sched_name:
            request.sched_name = sched_name.value

        for param in params:
            protoParam = request.parameters.add()
            protoParam.name = param["paramName"]
            protoParam.value = param["paramValue"]

        #
        # Should use _dataflow_service off of a client object but we don't have one or
        # we should execute the dataflow off the session object but we don't have one
        # of those either.  There is one hanging off of xcalarApi but it may be a legacy
        # session object.
        #
        return self.xcalarApi.sdk_client._dataflow_service.execute(request)

    def delete(self, name):
        workItem = WorkItemDeleteRetina(name)
        return self.xcalarApi.execute(workItem)

    def getDag(self, name):
        # return in Dag form
        workItem = WorkItemGetRetina(name)
        return self.xcalarApi.execute(workItem)

    def getDict(self, name):
        # return in Py Dict format
        workItem = WorkItemGetRetinaJson(name)
        out = self.xcalarApi.execute(workItem)
        return json.loads(out.retinaJson)

    def getGraph(self, name):
        # return as a Graph object
        query = self.getDict(name)["query"]
        return Graph(query)

    def update(self, name, retinaPyDict):
        retinaInJson = json.dumps(retinaPyDict)
        workItem = WorkItemUpdateRetina(name, retinaInJson)
        return self.xcalarApi.execute(workItem)
