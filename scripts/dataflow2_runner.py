#!/opt/xcalar/bin/python3.6
#
# Copyright 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

#
# This version is targeted at dataflow 2.0 dataflow workbook files.
#

import os
import traceback
import random
import logging

from xcalar.compute.util.config import build_config
from xcalar.external.Retina import Retina
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.client import Client

XCE_CONFIG = 'XCE_CONFIG'
PARAM_NAME = "paramName"
PARAM_VALUE = "paramValue"

"""Allows you to add dataflows and run them multiple times."""
class DataflowRunner(object):

    def __init__(self, username=None, workbook=None, xcalarenv='/etc/default/xcalar'):
        """Create a dataflow runner.

        The user can pass in a username and workbook or the dataflow creates it automatically.

        :param username: The username for a user.
        :param workbook: The name of a workbook.
        :param xcalarenv: The location of the xcalar default configuration.
        """
        self.xcalarApi = XcalarApi(bypass_proxy=True)
        self.username = username
        self.workbook = workbook
        self.xcalarenv = self._getEnv(xcalarenv)
        self.configDict = build_config(self.xcalarenv[XCE_CONFIG]).all_options
        self.dataflows = set()
        self.session = None

    def __enter__(self):
        if not self.username or not self.workbook:
            self.session = Session(self.xcalarApi, "DataflowRunner")
        else:
            self.session = Session(self.xcalarApi, self.username, self.username, userIdUnique=None, reuseExistingSession=True, sessionName=self.workbook)
        self.xcalarApi.setSession(self.session)
        self.dataflow = Retina(self.xcalarApi)
        return self

    def __exit__(self, type_, value, traceback):
        if self.session:
            self.session.destroy()

    def getXcalarApi(self):
        return self.xcalarApi

    def getConfigDict(self):
        return self.configDict

    def getXcalarEnv(self):
        return self.xcalarenv

    def _getEnv(self, envfile):
        """This allows us to know where xcalar is installed.

        :param envfile: The full path name of the xcalar env file.
        """
        lines = open(envfile, 'r').read().splitlines()
        envdict = {}
        for line in lines:
            line = line.strip()
            if line.startswith('#'):
                line = line[1:]
            if '=' in line:
                cols = line.split('=')
                if len(cols) == 2:
                    key,value = cols
                envdict[key.strip()] = value.strip()
        return envdict

    def _localExportUrl(self):
        """Construct the path of the local export."""
        return os.path.join(self.configDict["Constants.XcalarRootCompletePath"], "export")

    def latestExportDirStatus(self, exportName=""):
        """In case of file exports compute the location of the directory."""
        exportPath = os.path.join(self._localExportUrl(), exportName)
        return "Export Directory {}".format(exportPath)

    def run(self, dataflowPath, inside_session=None, params=None, dataflowName=None, exportTable=None):
        """Optionally add it and then run the dataflow.

        If the dataflow already exists, we will not create it. We will simply run it.
        We keep track of all the dataflows that were created by name.

        :param dataflowPath: Full path name string of the dataflow file.
        :param inside_session: a session created in which to run the dataflow
            and optionally leave resultant table
        :param params: A dictionary that contains the parameters to the dataflow file.
        :param dataflowName: The exact dataflowName of the dataflow that will be created.
        :param exportTable: The name of the resultant table from running the
            dataflow
        """
        if not dataflowName:
            dataflowName = os.path.basename(dataflowPath).split(".")[0]
        if dataflowName not in self.dataflows:
            dataflowContents = None
            with open(dataflowPath, "rb") as fp:
                dataflowContents = fp.read()
            uploaded_dataflow = self.client.add_dataflow(dataflowName,
                    dataflowContents)
        # The dataflow is executed in the session (inside_session) supplied by
        # the caller.
        try:
            ret = None
            if exportTable:
                self.client.execute_dataflow(dataflowName, inside_session,
                        params, exportTable)
                output = self.xcalarApi.listTable(exportTable)
                ret = output.nodeInfo[0].name
            else:
                ret = self.client.execute_dataflow(inside_session, dataflowName,
                        params)
        except:
            logging.error("Dataflow name: {}, error : {}".format(dataflowName, traceback.format_exc()))
            raise
        return ret
