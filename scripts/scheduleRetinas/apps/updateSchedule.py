import os
import sys
import json
import traceback
import logging
from datetime import datetime

XLRDIR = os.getenv("XLRDIR", "/opt/xcalar")

pathToScheduleApps = os.path.join(XLRDIR, "scripts", "scheduleRetinas", "apps")
pathToScheduleSupport = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                     "supportModules")
pathToScheduleCronExecute = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                         "executeFromCron")

sys.path.insert(0, pathToScheduleApps)
sys.path.insert(0, pathToScheduleSupport)
sys.path.insert(0, pathToScheduleCronExecute)

from kvMutex import KVMutex
from kvHandler import ScheduleKVHandler
from cronHandler import ScheduleCronHandler
from runAsApp import RunAsApp
from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Update Schedule App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Update Schedule App - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Update Schedule app initialized; hostname:{}".format(gethostname()))


### App for creating schedules and adding them to kv, cron
class ScheduleUpdater(object):
    def __init__(self, scheduleTag):
        self.kvHandler = ScheduleKVHandler(scheduleTag)
        self.cronHandler = ScheduleCronHandler(scheduleTag)
        self.kvMutex = KVMutex()

    def execute(self, jsonStr):
        pyObj = json.loads(jsonStr)
        scheduleName = pyObj["scheduleKey"]
        lockStr = self.kvMutex.getLock(scheduleName)
        if lockStr:
            try:
                if not self.kvHandler.hasSchedule(scheduleName):
                    # Entry with that ID already exists
                    self.kvHandler.releaseLock(lockStr)
                    return "-3"
                if not self.kvMutex.hasLock(lockStr):
                    return "-2"
                self.kvHandler.updateSchedule(pyObj, scheduleName)
            finally:
                self.kvMutex.releaseLock(lockStr)
            return "0"
        else:
            # Failed to lock
            return "-1"

def main(jsonStr):
    logger.info("Received input: {}".format(jsonStr))
    try:
        if RunAsApp().shouldRun():
            pyObj = json.loads(jsonStr)
            scheduleTag = "_XcalarScheduled" + pyObj.get("scheduleType", "Retina")
            scheduleUpdater = ScheduleUpdater(scheduleTag)
            statusCode = scheduleUpdater.execute(jsonStr)
            return statusCode
    except Exception as e:
        traceback = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": traceback})
