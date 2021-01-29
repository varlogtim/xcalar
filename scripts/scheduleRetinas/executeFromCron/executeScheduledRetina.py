import sys
import os
import json
import argparse
import time
import logging
import traceback
import random
import subprocess
import random
import math

from datetime import datetime

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Session import Session
from xcalar.external.LegacyApi.WorkItem import WorkItem, WorkItemGetTableMeta
from xcalar.external.Retina import Retina
from xcalar.external.exceptions import XDPException

from xcalar.compute.coretypes.Status.ttypes import *
from xcalar.compute.coretypes.LibApisCommon.ttypes import *
import executeSchedule

from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Execute Scheduled Retina Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Execute Scheduled Retina - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Execute Scheduled Retina initialized; hostname:{}".format(gethostname()))


class RetinaScheduler(executeSchedule.Scheduler):
    def __init__(self, scheduleName, scheduleTag, biweekly):
        super().__init__(scheduleName, scheduleTag, biweekly)

        self.xcalarApi = XcalarApi(bypass_proxy=True)
        inputInfo = self.kvHandler.getScheduleArguments(self.scheduleName)
        self.retName = inputInfo["retName"]
        self.parameters = inputInfo["substitutions"]
        self.options = inputInfo["options"]
        self.startTime = datetime.fromtimestamp(inputInfo["timingInfo"]["startTime"] / 1000.0)
        self.exportTarget = None
        self.exportLocation = None
        if "exportTarget" in list(self.options.keys()):
            self.exportTarget = self.options["exportTarget"]
        if "exportLocation" in list(self.options.keys()):
            self.exportLocation = self.options["exportLocation"]
        self.biweeklyModulo = self.getBiweeklyModulo(self.startTime)

        ## For active Session, change the configurations
        isActiveSession = self.options["activeSession"]
        if isActiveSession:
            self.session = Session(self.xcalarApi,"ScheduleUseOnly",
                    "XcalarInternalForTable", 5011028, True)
        else:
            self.session = Session(self.xcalarApi, "GarbageString",
                    "XcalarInternal", 4366255, True)
        self.xcalarApi.setSession(self.session)
        self.retina = Retina(self.xcalarApi)

        allResults = self.kvHandler.getScheduleResults(self.scheduleName)
        if allResults:
            lastResult = allResults[-1]
            nValue = 0
            for key, value in lastResult["parameters"].items():
                if key == "N":
                    nValue = value
                    break
            for key, value in self.parameters.items():
                if key == "N":
                    self.parameters[key] = nValue
                    break

    def execute(self):
        resultObj = self.executeRetina(self.retName, self.parameters,
                self.scheduleName, self.exportTarget, self.exportLocation)
        return resultObj

    def executeRetina(self, retName, parameters, scheduleName, exportTarget, exportLocation):
        logger.info("Retina Name {}, parameters {}, Schedule Name {}, export location {}".format(retName, parameters, scheduleName, exportLocation))

        nValue = 0
        for key, value in parameters.items():
            if key == "N":
                nValue = int(value) + 1
                parameters[key] = str(nValue)
                break

        retinaParams = []
        for key, value in parameters.items():
            paramDict = {"paramName" : str(key), "paramValue" : str(value)}
            retinaParams.append(paramDict)

        resultObj = {
            "randomNum" : random.uniform(1, 100),
            "startTime" : self.curTimeInMS(),
            "parameters": parameters,
            "exportTarget": exportTarget,
            "exportLocation": exportLocation
        }
        queryName = retName + "-" + str(random.randint(10000, 99999))

        ### Save the resultObj before execute the retina
        self.kvHandler.updateScheduleResults(scheduleName, resultObj)
        try:
            if self.options["activeSession"]:
                exportTableName = self.options["newTableName"] + "#" + str(nValue)
            else:
                exportTableName = None
            self.retina.execute(retName, retinaParams, queryName = queryName,
                    newTableName = exportTableName)
            resultObj["status"] = StatusT.StatusOk
        except XDPException as e:
            resultObj["status"] = e.statusCode
            print({"status": e.statusCode, "error": e})
        resultObj["endTime"] = self.curTimeInMS()

        ### Update the resultObj before execute the retina
        self.kvHandler.updateScheduleResults(self.scheduleName, resultObj)
        return resultObj
