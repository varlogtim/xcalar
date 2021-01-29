# Copyright 2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import sys

from xcalar.external.LegacyApi.WorkItem import WorkItemAppSet, WorkItemAppRun, WorkItemAppReap
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.compute.util.utils import XcUtil

from xcalar.compute.localtypes.App_pb2 import AppStatusRequest


class App(object):
    NotebookAppName = "NotebookApp"

    def __init__(self, client):
        self.client = client

    def set_py_app(self, name, exec_str):
        workItem = WorkItemAppSet(name, "Python", exec_str)
        return self.client._execute(workItem)

    def is_app_alive(self, group_id):
        req = AppStatusRequest()
        req.group_id = int(group_id)
        res = self.client._app_service.appStatus(req)
        return res.is_alive

    def run_py_app(self, name, is_global, in_str):
        app_group_id = self.run_py_app_async(name, is_global, in_str)
        return self.get_app_result(app_group_id, max_retries=sys.maxsize)

    def get_app_result(self,
                       app_group_id,
                       max_retries=0,
                       retry_sleep_timeout=1):
        workItem = WorkItemAppReap(app_group_id)
        retry_xc_codes = XcUtil.RETRY_XC_STATUS_CODES + [
            StatusT.StatusAppInProgress
        ]
        try:
            appOutput = XcUtil.retryer(
                self.client._execute,
                workItem,
                retry_exceptions=[],
                max_retries=max_retries,
                retry_sleep_timeout=retry_sleep_timeout,
                retry_xc_status_codes=retry_xc_codes)
            outStr = appOutput.output.outputResult.appReapOutput.outStr
            errStr = appOutput.output.outputResult.appReapOutput.errStr
        except Exception as e:
            if e.__class__ == XcalarApiStatusException:
                outStr = e.output.appReapOutput.outStr
                errStr = e.output.appReapOutput.errStr
            else:
                raise e

        return (outStr, errStr)

    # returns groupId of running app
    def run_py_app_async(self, name, is_global, in_str):
        workItem = WorkItemAppRun(name, is_global, in_str)
        outputRun = self.client._execute(workItem)

        return str(outputRun.output.outputResult.appRunOutput.appGroupId)

    def cancel(self, group_id):
        workItem = WorkItemAppReap(group_id, cancel=True)
        self.client._execute(workItem)
