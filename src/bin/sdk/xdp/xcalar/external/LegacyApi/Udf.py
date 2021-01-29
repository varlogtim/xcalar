# Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
from .WorkItem import WorkItemUdfAdd, WorkItemUdfUpdate
from .WorkItem import WorkItemUdfGet, WorkItemUdfDelete, WorkItemListXdfs
from Status.ttypes import StatusT
from .XcalarApi import XcalarApiStatusException
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiWorkbookScopeT
from xcalar.compute.localtypes.UDF_pb2 import GetResolutionRequest

class GetResCompat:
    def __init__(self, path):
        self.udfResPath = path

class Udf(object):
    def __init__(self, xcalarApi):
        self.xcalarApi = xcalarApi

    def add(self, moduleName, source):
        workItem = WorkItemUdfAdd(moduleName, source)
        try:
            result = self.xcalarApi.execute(workItem)
            assert result.moduleName == moduleName
            return result
        except XcalarApiStatusException as e:
            if e.status == StatusT.StatusUdfModuleLoadFailed:
                print("udf error:\n{}".format(
                    e.output.udfAddUpdateOutput.error.message))
            raise

    def update(self, moduleName, source):
        workItem = WorkItemUdfUpdate(moduleName, source)
        result = self.xcalarApi.execute(workItem)
        assert result.moduleName == moduleName
        return result

    def addOrUpdate(self, moduleName, source):
        try:
            self.add(moduleName, source)
        except XcalarApiStatusException as e:
            if e.status == StatusT.StatusUdfModuleAlreadyExists:
                self.update(moduleName, source)
            else:
                raise

    def get(self, moduleName):
        workItem = WorkItemUdfGet(moduleName)
        return self.xcalarApi.execute(workItem)

    #
    # Get a UDF's resolution (i.e. full path to the location from where the UDF
    # will be obtained during parsing, from the set of paths in UdfPath[] - see
    # include/udf/UserDefinedFunction.h).
    #
    # By default, the scope is session, which merely serves to provide the API
    # with its current session info (caller's username, and session name).
    #
    # However, caller can specify global scope
    # (XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeGlobal), which omits
    # caller's session info - this may be useful as an existence check in the
    # shared dir - the global scope forces the search to occur outside the
    # current session (i.e. in global scope aka /sharedUDFs).
    #
    # NOTE: (XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeUser isn't
    # implemented currently).
    def getRes(self,
               moduleName,
               scope=XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeSession):
        request = GetResolutionRequest()
        request.udfModule.name = moduleName
        if (scope == XcalarApiWorkbookScopeT.XcalarApiWorkbookScopeSession):
            request.udfModule.scope.workbook.name.username = self.xcalarApi.session.username
            request.udfModule.scope.workbook.name.workbookName = self.xcalarApi.session.name
        else:
            request.udfModule.scope.globl.SetInParent()
        result = self.xcalarApi.sdk_client._udf_service.getResolution(request)
        return GetResCompat(result.fqModName.text)

    def delete(self, moduleName):
        workItem = WorkItemUdfDelete(moduleName)
        return self.xcalarApi.execute(workItem)

    def list(self, fnNamePattern, CategoryPattern):
        workItem = WorkItemListXdfs(fnNamePattern, CategoryPattern)
        return self.xcalarApi.execute(workItem)
