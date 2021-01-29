import sys
import os
import json
import hashlib
import re
import time

XLRDIR = os.getenv("XLRDIR", "/opt/xcalar")

pathToScheduleApps = os.path.join(XLRDIR, "scripts", "scheduleRetinas", "apps")
pathToScheduleSupport = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                     "supportModules")
pathToScheduleCronExecute = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                         "executeFromCron")

sys.path.insert(0, pathToScheduleApps)
sys.path.insert(0, pathToScheduleSupport)
sys.path.insert(0, pathToScheduleCronExecute)

from xcalar.external.client import Client
from xcalar.external.kvstore import KvStore
from xcalar.external.exceptions import XDPException

from xcalar.compute.coretypes.Status.ttypes import *
from xcalar.compute.coretypes.LibApisCommon.ttypes import *

import logging

from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('KV handler Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - KV handler - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("KV handler initialized; hostname:{}".format(gethostname()))


### Helper functions for doing operations on kv schedules
### TODO: move all mutex related code out
### Locks are required to ensure that the same KV key isn't accessed
### By different threads simulatenously

### KV setup:
### {
###  "ScheduleKVList" : {
###         ,
###         '{"ScheduleKey":"ScheduleName1"}',
###         '{"ScheduleKey":"ScheduleName2"}',
###          ...
###          ...
###   }
###   "ScheduleName1-results":{},
###   "ScheduleName2-results":{},
### }

class ScheduleKVHandler(object):
    ######### Static ########
    initKVStr = ","
    ScheduleResultsTag = "-results"
    ScheduleListLock = "ScheduleKVLock"

    def __init__(self, scheduleTag, client=None, session=None, kvStore=None):
        # export as csv File
        if (client == None):
            self.client = Client(bypass_proxy=True)
        else:
            self.client = client
        if (kvStore == None):
            self.kvStore = self.client.global_kvstore()
        else:
            self.kvStore = kvStore
        self.session = session
        self.ScheduleListKey = "ScheduleKVList" + scheduleTag

    def cleanup(self):
        if self.session:
            self.session = None
        if self.client:
            self.client = None
        if self.kvStore:
            self.kvStore = None

#################### Begin Formatting KV Entries ###############

    def getResultKey(self, scheduleKey):
        return scheduleKey + self.ScheduleResultsTag

    ## ,str, to [str]
    ##  str  to [str]
    def commaWrapperToBracketWrapper(self, string):
        return "[" + string.strip(",") + "]"

    ## [str] to ,str,
    ##  str  to ,str,
    ##  "" to ","
    def bracketWrapperToCommaWrapper(self, string):
        # Length should be at least 2
        # Input should be a list
        if not string:
            return ","
        if (string[0] == "[" and string[-1] == "]"):
            string = string[1:-1]
        return "," + string + ","

    def appendComma(self, string):
        return string + ","
#################### End Formatting KV Entries #################
#################### Begin Schedule Initialization #############

    # Check whether Key "ScheduleKVList" exists in KvStore, if not, add it
    # in KvStore
    def checkMainList(self):
        try:
            self.kvStore.lookup(self.ScheduleListKey)
        except XDPException as e:
            if e.statusCode == StatusT.StatusKvEntryNotFound:
                self.kvStore.add_or_replace(self.ScheduleListKey, self.initKVStr, True)
            else:
                raise

#################### End Schedule Initialization ############
############### Begin Create, Delete, Update, Read, Pause and Resume Methods #####
    def createSchedule(self, pyObj):
        jsonStr = json.dumps(pyObj)
        scheduleName = pyObj["scheduleKey"]
        self.checkMainList()
        self.kvStore.append(self.ScheduleListKey, jsonStr + ",")
        self.kvStore.add_or_replace(scheduleName + "-results", self.initKVStr, True)

    def deleteSchedule(self, scheduleName, force=True):
        try:
            self.checkMainList()
            scheduleListArr = self.getScheduleList()
            newScheduleListArr = [schedule for schedule in scheduleListArr
                               if schedule.get("scheduleKey", None)
                               != scheduleName]
            self.setScheduleList(newScheduleListArr)
        except XDPException as e:
            if not e.statusCode == StatusT.StatusKvEntryNotFound:
                if force:
                    # If force, try to delete results as well but reraise
                    self.kvStore.delete(scheduleName + "-results")
                    raise
        try:
            self.kvStore.delete(scheduleName + "-results")
        except XDPException as e:
            if not e.statusCode == StatusT.StatusKvEntryNotFound:
                raise

    def updateSchedule(self, pyObj, scheduleName):
        self.checkMainList()
        scheduleListArr = self.getScheduleList()
        schedule = self.getSchedule(scheduleName, scheduleListArr)
        if schedule:
            schedule["timingInfo"] = pyObj["timingInfo"]
            schedule["substitutions"] = pyObj["substitutions"]
            schedule["options"] = pyObj["options"]
        self.setScheduleList(scheduleListArr)

    def readSchedule(self, scheduleName, hasRunResults):
        self.checkMainList()
        scheduleListArr = self.getScheduleList()
        schedule = self.getSchedule(scheduleName, scheduleListArr)
        retStruct = None
        if schedule is not None:
            retStruct = {}
            retStruct["scheduleMain"] = schedule
            if hasRunResults:
                retStruct["scheduleResults"] = self.getScheduleResults(scheduleName)
        return retStruct

    def readAllSchedules(self, hasRunResults):
        self.checkMainList()
        scheduleListArr = self.getScheduleList()
        allCombined = []
        for schedule in scheduleListArr:
            scheduleName = schedule.get("scheduleKey", None)
            if not scheduleName:
                continue
            scheduleInfo = {"scheduleMain": schedule}
            if hasRunResults:
                scheduleInfo["scheduleResults"] = self.getScheduleResults(scheduleName)
            allCombined.append(scheduleInfo)
        return allCombined

    def pauseSchedule(self, scheduleName):
        self.checkMainList()
        scheduleListArr = self.getScheduleList()
        schedule = self.getSchedule(scheduleName, scheduleListArr)
        if schedule:
            schedule["options"]["isPaused"] = True
        self.setScheduleList(scheduleListArr)

    def resumeSchedule(self, scheduleName):
        self.checkMainList()
        scheduleListArr = self.getScheduleList()
        schedule = self.getSchedule(scheduleName, scheduleListArr)
        if schedule:
            schedule["options"]["isPaused"] = False
        self.setScheduleList(scheduleListArr)

    ######################## Support Functions ######################################
    # The array of schedule information, each schedule is a String
    def getScheduleList(self):
        scheduleListStr = self.kvStore.lookup(self.ScheduleListKey)
        scheduleListArr = json.loads(self.commaWrapperToBracketWrapper(scheduleListStr))
        return scheduleListArr

    def setScheduleList(self, scheduleListArr):
        scheduleListStr = self.bracketWrapperToCommaWrapper(json.dumps(scheduleListArr))
        self.kvStore.add_or_replace(self.ScheduleListKey, scheduleListStr, True)

    def getSchedule(self, scheduleName, scheduleListArr):
        target = [schedule for schedule in scheduleListArr
                  if schedule.get("scheduleKey", None)
                  == scheduleName]
        if target:
            return target[0]
        else:
            return None

    def getScheduleResults(self, scheduleName):
        try:
            scheduleResultsStr = self.kvStore.lookup(scheduleName + "-results")
            scheduleResultsArr = json.loads(self.commaWrapperToBracketWrapper(scheduleResultsStr))
            return scheduleResultsArr
        except XDPException as e:
            if (e.statusCode != StatusT.StatusKvEntryNotFound):
                raise
            else:
                return None

    def setScheduleResults(self, scheduleName, scheduleResultsArr):
        scheduleResultsStr = self.bracketWrapperToCommaWrapper(json.dumps(scheduleResultsArr))
        self.kvStore.add_or_replace(scheduleName + "-results", scheduleResultsStr, True)

    def updateScheduleResults(self, scheduleName, resultObj):
        if 'endTime' in resultObj:
             ### After end, add new properties
            scheduleResultsArr = self.getScheduleResults(scheduleName)
            for result in scheduleResultsArr:
                if ('randomNum' in result) and (result["randomNum"] == resultObj["randomNum"]):
                    for k,v in resultObj.items():
                        result[k] = v
                    break
            self.setScheduleResults(scheduleName, scheduleResultsArr)
        else:
            ### Before Start, append the new Record
            self.kvStore.append(scheduleName + "-results", json.dumps(resultObj) + ",")

    def hasSchedule(self, scheduleName):
        self.checkMainList()
        scheduleList = self.getScheduleList()
        schedule = self.getSchedule(scheduleName, scheduleList)
        if schedule:
            return True
        return False

    def getScheduleArguments(self, scheduleName):
        # When support variable argument options, change this
        # For now, need: retName, substitutions, options
        self.checkMainList()
        scheduleList = self.getScheduleList()
        if not scheduleList:
            return None
        return self.getSchedule(scheduleName, scheduleList)

    def getAllScheduleNames(self):
        scheduleListArr = self.getScheduleList()
        scheduleNames = [schedule["scheduleKey"] for schedule in scheduleListArr
                        if schedule.get("scheduleKey", None) is not None]
        return scheduleNames

    def areResultsEmpty(self, scheduleName):
        if not self.getScheduleResults(scheduleName):
            return True
        return False

##########################################################

############### DANGER ZONE ##############################

    def deleteAllFromKV(self):
        # DO NOT DO THIS.  TESTING ONLY.
        try:
            allKeys = self.getAllScheduleNames()
            print("Deleting results")
            for scheduleName in allKeys:
                try:
                    self.kvStore.delete(scheduleName + "-results")
                except Exception as e:
                    pass
        except Exception as e:
            pass
        print("Deleting main")
        try:
            self.kvStore.delete(self.ScheduleListKey)
        except Exception as e:
            pass
        print("Deleting lock")
        try:
            self.kvStore.delete(self.ScheduleListLock)
        except Exception as e:
            pass

