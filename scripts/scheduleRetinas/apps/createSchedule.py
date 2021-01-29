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
logger = logging.getLogger('Create Schedule App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Create Schedule App - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Create Schedule app initialized; hostname:{}".format(gethostname()))


### App for creating schedules and adding them to kv, cron

class ScheduleCreator(object):
    def __init__(self, scheduleTag):
        self.kvHandler = ScheduleKVHandler(scheduleTag)
        self.kvMutex = KVMutex()
        self.cronHandler = ScheduleCronHandler(scheduleTag)

    def execute(self, jsonStr):
        pyObj = json.loads(jsonStr)
        scheduleName = pyObj["scheduleKey"]

        # Generate a Lock
        lockStr = self.kvMutex.getLock(scheduleName)
        if lockStr:
            try:
                # Schedule Already exist
                if self.kvHandler.hasSchedule(scheduleName):
                    self.kvMutex.releaseLock(lockStr)
                    return "-3"

                self.cronHandler.createSchedule(pyObj)
                # Lost Lock, abort
                if not self.kvMutex.hasLock(lockStr):
                    self.cronHandler.deleteSchedule(scheduleName)
                    return "-2"
                self.kvHandler.createSchedule(pyObj)

            finally:
                self.kvMutex.releaseLock(lockStr)

            # Successfully
            return "0"
        else:
            # Failed to lock
            return "-1"

### jsonStr should have the format as:
### {
###     "scheduleKey":"bdf",
###     "scheduleType":"Retina"
###     "retName":"bdf",
###     "substitutions":
###     [{
###         "paramName":"N",
###         "paramValue":0
###     }],
###     "options":
###     {
###         "activeSession":false,
###         "newTableName":"",
###         "usePremadeCronString":false,
###         "premadeCronString":"",
###         "isPaused":false
###     },
###     "timingInfo":
###     {
###         "startTime":1499137260000,
###         "dateText":"7/4/2017",
###         "timeText":"03 : 01 AM",
###         "repeat":"minute",
###         "modified":1499137226726,
###         "created":1499137226726
###     }
### }
def main(jsonStr):
    logger.info("Received input: {}".format(jsonStr))
    try:
        if RunAsApp().shouldRun():
            pyObj = json.loads(jsonStr)
            scheduleTag = "_XcalarScheduled" + pyObj.get("scheduleType", "Retina")
            scheduleCreator = ScheduleCreator(scheduleTag)
            statusCode = scheduleCreator.execute(jsonStr)
            return statusCode
    except Exception as e:
        traceback = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": traceback})
