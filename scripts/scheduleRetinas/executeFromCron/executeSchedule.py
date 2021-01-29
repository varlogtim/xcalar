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

XLRDIR = os.getenv("XLRDIR", "/opt/xcalar")

pathToScheduleApps = os.path.join(XLRDIR, "scripts", "scheduleRetinas", "apps")
pathToScheduleSupport = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                     "supportModules")
pathToScheduleCronExecute = os.path.join(XLRDIR, "scripts", "scheduleRetinas",
                                         "executeFromCron")

sys.path.insert(0, pathToScheduleApps)
sys.path.insert(0, pathToScheduleSupport)
sys.path.insert(0, pathToScheduleCronExecute)

from cronHandler import ScheduleCronHandler
from kvHandler import ScheduleKVHandler
from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Execute Schedule Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Execute Schedule - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Execute Schedule initialized; hostname:{}".format(gethostname()))


class Scheduler(object):
    def __init__(self, scheduleName, scheduleTag, biweekly):
        self.scheduleName = scheduleName
        self.scheduleTag = scheduleTag
        self.curTime = datetime.now()
        self.biweekly = biweekly
        self.kvHandler = ScheduleKVHandler(scheduleTag)

    @staticmethod
    def getSchedulerInstance(scheduleName, scheduleTag, biweekly):
        logger.info("getSchedulerInstance scheduleName {}, scheduleTag {}, biweekly {}".format(scheduleName, scheduleTag, biweekly))
        if scheduleTag == "_XcalarScheduledRetina":
            import executeScheduledRetina
            return executeScheduledRetina.RetinaScheduler(scheduleName, scheduleTag, biweekly)
        elif scheduleTag == "_XcalarScheduledSnapshot":
            import executeScheduledSnapshot
            return executeScheduledSnapshot.SnapshotScheduler(scheduleName, scheduleTag, biweekly)
        else:
            raise ValueError("Invalid schedule type!")

    # get the millisecond accuracy
    @staticmethod
    def curTimeInMS():
        return int(time.time() * 1000)

    @staticmethod
    def getBiweeklyModulo(someTime):
        weeksSinceEpoch = (someTime - datetime(1970,1,1)).days // 7
        return weeksSinceEpoch % 2

    def isItTime(self):
        # Biweekly is boolean flag from argparse
        if self.biweekly and \
            (self.getBiweeklyModulo(self.curTime) != self.biweeklyModulo):
            return False
        if self.curTime > self.startTime and \
                self.options["isPaused"] is False:
            return True
        return False

    def main(self):
        logger.info("Execute Schedule: Schedule Name {}, curTime {}, biweekly {}".format(self.scheduleName, self.curTime, self.biweekly))
        if self.isItTime():
            return self.execute()
        else:
            return None

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Processes schedule key.")
        parser.add_argument("cronKey", default="",
                            help="schedule name")
        parser.add_argument("--biweekly", help="if supplied runs biweekly",
                            action="store_true")
        cronArgs = parser.parse_args()
        cronKey = cronArgs.cronKey
        biweekly = cronArgs.biweekly
        scheduleName, scheduleTag = cronKey.rsplit("_", 1)
        scheduleTag = '_' + scheduleTag
        p = subprocess.Popen(['pgrep', 'usrnode'])
        p.communicate()
        if p.returncode == 0:
            schedle = Scheduler.getSchedulerInstance(scheduleName=scheduleName,
                    scheduleTag=scheduleTag, biweekly=biweekly)
            schedle.main()
    except Exception as e:
        traceback = traceback.format_exc()
        print({"status": -1, "error": e, "traceback": traceback})
