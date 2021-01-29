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
from runAsApp import RunAsApp
from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Resume Schedule App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Resume Schedule App - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Resume Schedule app initialized; hostname:{}".format(gethostname()))


### App for resuming schedules

class ScheduleResumer(object):
    def __init__(self, scheduleTag):
        self.kvHandler = ScheduleKVHandler(scheduleTag)
        self.kvMutex = KVMutex()

    def execute(self, jsonStr):
        pyObj = json.loads(jsonStr)
        scheduleName = pyObj["scheduleKey"]
        lockStr = self.kvMutex.getLock(scheduleName)
        if lockStr:
            try:
                if not self.kvMutex.hasLock(lockStr):
                    return "-2"
                self.kvHandler.resumeSchedule(scheduleName)
            finally:
                self.kvMutex.releaseLock(lockStr)
            return "0"
        else:
            # Failed to get lock
            return "-1"

def main(jsonStr):
    logger.info("Received input: {}".format(jsonStr))
    try:
        if RunAsApp().shouldRun():
            inObj = json.loads(jsonStr)
            scheduleTag = "_XcalarScheduled" + inObj.get("scheduleType", "Retina")
            scheduleResumer = ScheduleResumer(scheduleTag)
            statusCode = scheduleResumer.execute(jsonStr)
            return statusCode
    except Exception as e:
        traceback = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": traceback})
