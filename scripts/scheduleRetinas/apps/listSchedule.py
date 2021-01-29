import os
import sys
import json
import subprocess
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

from kvHandler import ScheduleKVHandler
from cronHandler import ScheduleCronHandler
from runAsApp import RunAsApp
from checkConsistency import ConsistencyChecker
from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('List Schedule App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - List Schedule App - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("List Schedule app initialized; hostname:{}".format(gethostname()))


### App for listing schedules.
### Takes either exact schedule key to list that schedule if it exists
### or empty string to list all schedules (does not yet support patterns)

class ScheduleLister(object):
    def __init__(self, scheduleTag):
        self.kvHandler = ScheduleKVHandler(scheduleTag)
        self.cronHandler = ScheduleCronHandler(scheduleTag)
    # Because this will eventually support patterns, all results returned as
    # lists.  However, internally, kvHandler and cronHandler return empty as
    # none.

    def execute(self, jsonStr):
        pyObj = json.loads(jsonStr)
        scheduleName = pyObj.get("scheduleKey", "")
        hasRunResults = pyObj.get("hasRunResults", False)
        if not scheduleName:
            scheduleName = None
        if scheduleName:
            # Get information of one schedule
            tmp = self.kvHandler.readSchedule(scheduleName, hasRunResults)
            if not tmp:
                return []
            return [tmp]
        else:
            # Get information of all schedules
            return self.kvHandler.readAllSchedules(hasRunResults)

def main(jsonStr):
    logger.info("Received input: {}".format(jsonStr))
    try:
        if RunAsApp().shouldRun():
            pyObj = json.loads(jsonStr)
            scheduleTag = "_XcalarScheduled" + pyObj.get("scheduleType", "Retina")
            scheduleLister = ScheduleLister(scheduleTag)
            listResults = scheduleLister.execute(jsonStr)
            return json.dumps(listResults)
    except Exception as e:
        traceback = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": traceback})
