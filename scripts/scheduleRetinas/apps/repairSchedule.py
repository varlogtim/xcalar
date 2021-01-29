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

from kvHandler import ScheduleKVHandler
from cronHandler import ScheduleCronHandler
from runAsApp import RunAsApp
from checkConsistency import ConsistencyChecker
from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Repair Schedule App Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Repair Schedule App - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Repair Schedule app initialized; hostname:{}".format(gethostname()))


# TODO: test from frontend

class scheduleRepairer(object):
    def __init__(self, scheduleTag):
        self.kvHandler = ScheduleKVHandler(scheduleTag)
        self.kvMutex = kvHandler.kvMutex()
        self.cronHandler = ScheduleCronHandler(scheduleTag)
        self.consistencyChecker = ConsistencyChecker(self.kvHandler,
                                                     self.cronHandler)

    def cleanup(self):
        self.kvHandler.cleanup()
        self.kvMutex.cleanup()
        self.cronHandler.cleanup()
        self.consistencyChecker.cleanup()

    def repairSchedule(self, forceReleaseLock=False):
        if forceReleaseLock:
            lockRes = self.kvMutex.forceTakeLock("ScheduleRepair")
        else:
            lockRes = self.kvMutex.getLock("ScheduleRepair")
        if lockRes:
            try:
                self.consistencyChecker.consistencyRepair()
                # TODO: when UX for repairing schedule comes through, uncomment
                # below
                # assumptions = self.consistencyChecker.consistencyCheck().values()
                # if not all(assumptions.values()):
            finally:
                self.kvMutex.releaseLock(lockRes)
            return "0"
        else:
            # Couldn't get lock
            return "-1"

def main(inBlob):
    logger.info("Received input: {}".format(inBlob))
    try:
        if RunAsApp().shouldRun():
            inObj = json.loads(inBlob)
            scheduleTag = "_XcalarScheduled" + inObj.get("scheduleType", "Retina")
            forceReleaseLock = inObj.get("forceReleaseLock", False)
            scheduleRepairer = ScheduleRepairer(scheduleTag)
            return json.dumps(scheduleRepairer.repairSchedule(forceReleaseLock))
    except Exception as e:
        traceback = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": traceback})
