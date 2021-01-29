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
logger = logging.getLogger('Delete Schedule App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Delete Schedule App - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Delete Schedule app initialized; hostname:{}".format(gethostname()))


### App for deleting schedules from kv and cron

class ScheduleDeleter(object):
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
                self.cronHandler.deleteSchedule(scheduleName)
                # Lost Lock, abort
                if not self.kvMutex.hasLock(lockStr):
                    schedule = self.kvHandler.getSchedule(scheduleName)
                    if schedule:
                        self.cronHandler.createSchedule(schedule)
                    return "-2"
                # Delete Schedule
                self.kvHandler.deleteSchedule(scheduleName)
            finally:
                self.kvMutex.releaseLock(lockStr)
            # Successfully
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
            scheduleDeleter = ScheduleDeleter(scheduleTag)
            statusCode = scheduleDeleter.execute(jsonStr)
            return statusCode
    except Exception as e:
        tb = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": tb})
