# Copyright 2016-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import time
from urllib.parse import urlparse, urlunparse
import urllib3

from thrift.transport import THttpClient
from thrift.protocol import TJSONProtocol

from xcalar.compute.coretypes.LibApisCommon import XcalarApiService
from xcalar.compute.coretypes.LibApisCommon.ttypes import (XcalarApiException)

from xcalar.compute.coretypes.LibApisEnums.ttypes import XcalarApisT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.Status.constants import StatusTStr
from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT
from xcalar.compute.coretypes.DagStateEnums.ttypes import DgDagStateT
from xcalar.compute.coretypes.QueryStateEnums.ttypes import QueryStateT

from xcalar.compute.localtypes.Dataflow_pb2 import ExecuteRequest
from xcalar.compute.localtypes.Query_pb2 import ListRequest
from xcalar.external.exceptions import XDPException

from .WorkItem import (
    WorkItemGetConfigParams, WorkItemGetOpStats, WorkItemGetTableMeta,
    WorkItemListDagInfo, WorkItemListXdfs, WorkItemQueryCancel,
    WorkItemQueryDelete, WorkItemQueryState,
    WorkItemRenameNode, WorkItemSetConfigParam, WorkItemUdfAdd)
from xcalar.compute.coretypes.FunctionCategory.ttypes import FunctionCategoryT
from xcalar.compute.coretypes.FunctionCategory.constants import FunctionCategoryTStr

from xcalar.compute.util.utils import XcUtil

from .AuthUtil import Authenticate
from .Env import XcalarMgmtdUrl

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#
# Class for interacting with Xcalar backend.
#


# An exception representing an API call that didn't return StatusOk. XcalarApi output
# stored in addition to failure status.
class XcalarApiStatusException(XcalarApiException):
    """Low level exception for the core thrift client"""

    def __init__(self, status, output, result=None):
        super(XcalarApiStatusException, self).__init__(status)
        if status in StatusTStr:
            self.message = StatusTStr[status]
        else:
            self.message = "Unknown message (%d)" % status
        self.output = output
        self.result = result

    def __str__(self):
        return self.message

    # https://issues.apache.org/jira/browse/THRIFT-4002
    def __hash__(self):
        # we don't need self.result here because  it is a subset of self.output
        # repr might have some issues here
        return 101 * hash(self.status) + hash(repr(self.output))


class XcalarApi(object):
    def __init__(self,
                 url=None,
                 client_secrets=None,
                 client_secrets_file=None,
                 bypass_proxy=False,
                 client_token=None,
                 session_type='api',
                 save_cookies=False,
                 auth_instance=None,
                 sdk_client=None):
        if auth_instance is not None and not isinstance(
                auth_instance, Authenticate):
            raise TypeError(
                "auth_instance must be Authenticate object, not '{}'".format(
                    type(auth_instance)))
        if auth_instance is None:
            self.auth = Authenticate(
                url=url,
                client_secrets=client_secrets,
                client_secrets_file=client_secrets_file,
                bypass_proxy=bypass_proxy,
                client_token=client_token,
                session_type=session_type,
                save_cookies=save_cookies)
        else:
            self.auth = auth_instance

        if self.auth._using_proxy:
            # building the proxy url using service endpoint url path
            service_url = self.auth.parsed_url._replace(
                path=urlparse(XcalarMgmtdUrl).path)
            rendered_url = urlunparse(service_url)
        else:
            rendered_url = XcalarMgmtdUrl

        self.transport = THttpClient.THttpClient(rendered_url)
        self.protocol = TJSONProtocol.TJSONProtocol(self.transport)
        self.client = XcalarApiService.Client(self.protocol)
        self.session = None
        if (sdk_client == None):
            #
            # Import here to avoid import loop with client.py who includes us.
            #
            from xcalar.external.client import Client as SDKClient
            self.sdk_client = SDKClient(url=url,
                                        client_secrets=client_secrets,
                                        client_secrets_file=client_secrets_file,
                                        client_token=client_token,
                                        bypass_proxy=True,
                                        session_type=session_type)
        else:
            self.sdk_client = sdk_client

    def execute(self, workItem):
        if self.session:
            workItem.workItem.sessionName = self.session.name
            workItem.workItem.userIdUnique = self.session.userIdUnique
            workItem.workItem.userId = self.session.username
        elif not workItem.workItem.userIdUnique:
            workItem.workItem.userIdUnique = 1
            workItem.workItem.userId = "api user"

        if self.auth.is_session_expired():
            self.auth.login()
        cookie_str = self.auth.get_cookie_string()

        headers = {"Cookie": cookie_str}
        headers.update({"Connection": "keep-alive"})
        self.transport.setCustomHeaders(headers)
        self.transport.open()
        try:
            try:
                result = self.client.queueWork(workItem.workItem)
            except XcalarApiException as e:
                newExc = XcalarApiStatusException(e.status, "unknown output")
                raise newExc
            finally:
                self.transport.close()
            if result.output.hdr.status != StatusT.StatusOk:
                if result.output.hdr.log:
                    print(result.output.hdr.log)
                raise XcalarApiStatusException(result.output.hdr.status,
                                               result.output.outputResult,
                                               result)
        except Exception as e:
            # XXX pytest uses traceback to try to print this error. Internally,
            # traceback examines __context__ and inserts it into a hash table.
            # In doing so, it will call hash(__context__). The issue here is that
            # __context__ (which is the thrift-generated XcalarApiException)
            # does not implement __hash__, so traceback itself throws an exception.
            # Here we are doing a complicated dance to unset __context__, allowing
            # pytest to successfully handle this exception.
            e.__context__ = None
            raise
        return workItem.output(result)

    def setSession(self, session):
        self.session = session

    def listDataset(self, namePattern, userName=None, userIdUnique=None):
        workItem = WorkItemListDagInfo(namePattern, SourceTypeT.SrcDataset,
                                       userName, userIdUnique)
        return self.execute(workItem)

    # shortcut to list table
    def listTable(self, tableNamePattern, userName=None, userIdUnique=None):
        workItemListTable = WorkItemListDagInfo(
            tableNamePattern, SourceTypeT.SrcTable, userName, userIdUnique)
        return self.execute(workItemListTable)

    def listConstant(self, tableNamePattern, userName=None, userIdUnique=None):
        workItemListTable = WorkItemListDagInfo(
            tableNamePattern, SourceTypeT.SrcConstant, userName, userIdUnique)
        return self.execute(workItemListTable)

    def cancelQuery(self, queryName, userName=None, userIdUnique=None):
        workItemCancel = WorkItemQueryCancel(queryName, userName, userIdUnique)
        return self.execute(workItemCancel)

    def listQueries(self, namePattern, userName=None, userIdUnique=None):
        request = ListRequest()
        request.name_pattern = namePattern
        return self.sdk_client._query_service.list(request)

    # shortcut to submit query
    # XXX: this interface shouldn't be called by applications invoking the SDK - they should all
    # call the execute_dataflow method in the client or session class. The queryName passed
    # below is the exact name the back-end will use to execute the query - and so can be used
    # to manage the query (invoke delete or queryStatea APIs using this name).
    def submitQuery(self,
                    queryStr,
                    sessionName=None,
                    queryName=None,
                    userName=None,
                    userIdUnique=None,
                    overwriteIfExists=True,
                    bailOnError=True,
                    schedName="",
                    isAsync=False,
                    pinResults=False,
                    udfUserName="",
                    udfSessionName="",
                    collectStats=False):
        if queryName is None and isAsync is False:
            # user doesn't care about query name, use a tmp one and delete it afterwards
            queryName = "XcalarSDKTempQuery"

        if sessionName is None:
            sessionName = self.session.name
        if userName is None:
            userName = self.session.username

        req = ExecuteRequest()
        req.dataflow_str = queryStr
        req.job_name = queryName
        req.scope.workbook.name.username = userName
        req.scope.workbook.name.workbookName = sessionName
        req.udf_user_name = udfUserName
        req.udf_session_name = udfSessionName
        req.is_async = isAsync
        req.sched_name = schedName
        req.collect_stats = collectStats
        req.pin_results = pinResults
        req.optimized = False

        try:
            output = self.sdk_client._dataflow_service.execute(req)
            if queryName == "XcalarSDKTempQuery":
                self.deleteQuery(queryName, userName, userIdUnique)
            return output
        except XDPException as e:
            if (e.statusCode == StatusT.StatusQrQueryAlreadyExists and overwriteIfExists):
                self.deleteQuery(queryName, userName, userIdUnique)
                try:
                    return DataflowClient().execute(req)
                except XDPException as e:
                    raise XcalarApiStatusException(e.statusCode, None)
            else:
                raise XcalarApiStatusException(e.statusCode, None)

    def queryState(self, queryName, detailedStats=True):
        workItemQueryState = WorkItemQueryState(queryName, detailedStats)
        return self.execute(workItemQueryState)

    def _queryDone(self, queryState):
        return (queryState == QueryStateT.qrFinished
                or queryState == QueryStateT.qrCancelled
                or queryState == QueryStateT.qrError)

    def waitForQuery(self,
                     queryName,
                     userName=None,
                     userIdUnique=None,
                     pollIntervalInSecs=1):
        workItemQueryState = WorkItemQueryState(queryName, False, userName,
                                                userIdUnique)
        output = None
        while True:
            output = XcUtil.retryer(self.execute, workItemQueryState)
            if self._queryDone(output.queryState) is True:
                break
            time.sleep(pollIntervalInSecs)
        return output

    def deleteQuery(self,
                    queryName,
                    userName=None,
                    userIdUnique=None,
                    deferredDelete=True):
        workItemQueryDelete = WorkItemQueryDelete(queryName, userName,
                                                  userIdUnique)
        output = None
        queryExisted = False
        markedForDeletionStatuses = [
            StatusT.StatusStatsCollectionInProgress,
            StatusT.StatusQrQueryAlreadyDeleted
        ]
        while True:
            try:
                output = self.execute(workItemQueryDelete)
                break
            except XcalarApiStatusException as e:
                if not deferredDelete and e.status in markedForDeletionStatuses:
                    queryExisted = True
                elif (deferredDelete and e.status in markedForDeletionStatuses
                      ) or (not deferredDelete and queryExisted
                            and e.status == StatusT.StatusQrQueryNotExist):
                    break
                else:
                    raise
            time.sleep(0.1)
        return output

    def loadUdf(self,
                moduleName,
                fnName,
                filePath,
                username=None,
                userIdUnique=None):
        found = False
        fullFnName = "%s:%s" % (moduleName, fnName)
        category = FunctionCategoryTStr[FunctionCategoryT.FunctionCategoryUdf]
        listUdfWorkItem = WorkItemListXdfs(fullFnName, category, username,
                                           userIdUnique)
        output = self.execute(listUdfWorkItem)
        if output.numXdfs > 0:
            for fn in output.fnDescs:
                if fn.fnName == fullFnName:
                    found = True

        if not found:
            with open(filePath) as f:
                addUdfWorkItem = WorkItemUdfAdd(moduleName, f.read(), username,
                                                userIdUnique)
                output = self.execute(addUdfWorkItem)

    def getOpStats(self, name, userName=None, userIdUnique=None):
        workItemOpStats = WorkItemGetOpStats(name, userName, userIdUnique)
        return self.execute(workItemOpStats)

    def getOpName(self, name, userName=None, userIdUnique=None):
        operation = self.getOpStats(name, userName, userIdUnique).api
        operation = XcalarApisT._VALUES_TO_NAMES[int(operation)]
        return operation

    def getTableMeta(self, isTable, name, userName=None, userIdUnique=None):
        workItem = WorkItemGetTableMeta(not isTable, name, userName,
                                        userIdUnique)
        return self.execute(workItem)

    def getConfigParams(self, userName=None, userIdUnique=None):
        workItem = WorkItemGetConfigParams(userName, userIdUnique)
        return self.execute(workItem)

    def setConfigParam(self,
                       paramName,
                       paramValue,
                       userName=None,
                       userIdUnique=None):
        workItem = WorkItemSetConfigParam(paramName, paramValue, userName,
                                          userIdUnique)
        return self.execute(workItem)

    def isQueryReady(self, state):
        return state == DgDagStateT._NAMES_TO_VALUES["DgDagStateReady"]

    def renameNode(self, oldName, newName, userName=None, userIdUnique=None):
        workItem = WorkItemRenameNode(oldName, newName, userName, userIdUnique)
        return self.execute(workItem)
