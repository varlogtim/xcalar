import os
import sys
import json
import subprocess
import re
from datetime import datetime
import logging

from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Cron handler Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Cron handler - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Cron handler initialized; hostname:{}".format(gethostname()))

### Handles all cron-related tasks including formatting,
### creating, listing, and deleting schedules in cron

class ScheduleCronHandler(object):
    def __init__(self, cronTableTag):
        self.cronTableTag = cronTableTag

    def cleanup(self):
        pass

    def cronKeyToScheduleKey(self, cronKey):
        if not cronKey.endswith(self.cronTableTag):
            return None
        return cronKey[:-len(self.cronTableTag)]

    def scheduleKeyToCronKey(self, scheduleKey):
        if scheduleKey.endswith(self.cronTableTag):
            return None
        return scheduleKey + self.cronTableTag

    def getScheduleInfoFromLine(self, cronLine):
        # Format: * * * * * python executorPath scheduleName --biweekly
        timingParamsMatch = re.search("^([\*\w,]\s){5}?", cronLine)
        cronKeyMatch = re.search("[^\s]+" + re.escape(self.cronTableTag) +
                                 "[\s$\)]", cronLine)
        biweeklyMatch = re.search(re.escape("--biweekly"), cronLine)
        if (timingParamsMatch and cronKeyMatch):
            timingParams = cronLine[timingParamsMatch.start(): \
                                    timingParamsMatch.end()].strip()
            cronKey = cronLine[cronKeyMatch.start(): \
                               cronKeyMatch.end()].strip()
            # TODO: This is jenky.  make the match work better for )
            if cronKey.endswith(")"):
                cronKey = cronKey[:-1]
            scheduleKey = self.cronKeyToScheduleKey(cronKey)
            isBiweekly = True if biweeklyMatch else False
            retObj = {
                "cronTimingParams" : timingParams,
                "scheduleKey" : scheduleKey,
                "isBiweekly"  : isBiweekly
            }
            return retObj
        else:
            return None

    def createSchedule(self, scheduleObj):
        if scheduleObj["options"].get("usePremadeCronString", False):
            cronStr = self.makeFullCronString(scheduleObj["timingInfo"], scheduleObj["scheduleKey"],
                      scheduleObj["options"]["premadeCronString"]);
        else:
            cronStr = self.makeFullCronString(scheduleObj["timingInfo"],
                                              scheduleObj["scheduleKey"])
        logger.info("createSchedule: cronStr {}".format(cronStr))
        os.system("(crontab -l ; echo '" + cronStr + "') | crontab -")

    def deleteSchedule(self, scheduleName):
        cronStr = self.scheduleKeyToCronKey(scheduleName)
        logger.info("deleteSchedule: cronStr {}".format(cronStr))
        os.system("crontab -l | grep -v '" + cronStr + "' | crontab -")

    def findAllSchedulesInCron(self):
        cronTable = subprocess.check_output(["crontab", "-l"]).decode("utf-8")
        schedulesInCron = []
        for line in cronTable.split("\n"):
            if line.find(self.cronTableTag) != -1:
                schedInfo = self.getScheduleInfoFromLine(line)
                if schedInfo is not None:
                    schedulesInCron.append(schedInfo)

        return schedulesInCron

    def findScheduleInCron(self, scheduleKey):
        allSched = self.findAllSchedulesInCron()
        oneSched = [cronInfo for cronInfo in allSched
                    if cronInfo.get("scheduleKey", None) == scheduleKey]
        if (oneSched):
            return oneSched[0]
        else:
            return None

    #### Functions for creating schedule strings
    def getBaseScriptStr(self, scheduleKey):
        xlrDir = os.environ['XLRDIR']
        # Use the env variable PYTHONHOME if available to construct the python
        # executable path.
        if 'PYTHONHOME' in os.environ:
            pyExec = os.path.join(os.environ["PYTHONHOME"], "bin", "python3.6")
        else:
            pyExec = sys.executable

        scriptStr = "{} {executeFile} {cronKey}".format(
                pyExec,
                executeFile = os.path.join(xlrDir,
                                "scripts/scheduleRetinas/executeFromCron/executeSchedule.py"),
                cronKey = self.scheduleKeyToCronKey(scheduleKey))
        return scriptStr

    def makeCronStr(self, cronStrArgs):
        return " ".join([cronStrArgs["minute"],
                         cronStrArgs["hour"],
                         cronStrArgs["dayOfMonth"],
                         cronStrArgs["month"],
                         cronStrArgs["dayOfWeek"]])

    def convertTimingToCronStr(self, timingInfo, startTime):
        def weekday(dtObj):
            return dtObj.weekday() + 1
        timingSuffix = ""
        repeat = timingInfo["repeat"]
        modified = timingInfo["modified"]
        cronStrArgs = {
            "minute" : "",
            "hour" : "",
            "dayOfMonth" : "",
            "month" : "",
            "dayOfWeek" : ""
        }
        if (repeat == "minute"):
            cronStrArgs["minute"] = "*"
            cronStrArgs["hour"] = "*"
            cronStrArgs["dayOfMonth"] = "*"
            cronStrArgs["month"] = "*"
            cronStrArgs["dayOfWeek"] = "*"
        elif (repeat == "hourly"):
            cronStrArgs["minute"] = str(startTime.minute)
            cronStrArgs["hour"] = "*"
            cronStrArgs["dayOfMonth"] = "*"
            cronStrArgs["month"] = "*"
            cronStrArgs["dayOfWeek"] = "*"
        elif (repeat == "daily"):
            cronStrArgs["minute"] = str(startTime.minute)
            cronStrArgs["hour"] = str(startTime.hour)
            cronStrArgs["dayOfMonth"] = "*"
            cronStrArgs["month"] = "*"
            cronStrArgs["dayOfWeek"] = "*"
        elif (repeat == "weekly"):
            cronStrArgs["minute"] = str(startTime.minute)
            cronStrArgs["hour"] = str(startTime.hour)
            cronStrArgs["dayOfMonth"] = "*"
            cronStrArgs["month"] = "*"
            cronStrArgs["dayOfWeek"] = str(weekday(startTime))
        elif (repeat == "biweekly"):
            # Biweekly case is handled in retina execute script
            timingSuffix += "--biweekly"
            cronStrArgs["minute"] = str(startTime.minute)
            cronStrArgs["hour"] = str(startTime.hour)
            cronStrArgs["dayOfMonth"] = "*"
            cronStrArgs["month"] = "*"
            cronStrArgs["dayOfWeek"] = str(weekday(startTime))

        cronStr = self.makeCronStr(cronStrArgs)

        return cronStr, timingSuffix

    def validateCronStr(self, premadeCronStr):
        # TODO: ensure that string is a valid cron schedule string,
        # and is not ending in whitespace
        return True

    def makeFullCronString(self, timingInfo, scheduleKey, premadeCronStr = None):
        # startTime should be the startTime field in timingInfo
        baseScript = self.getBaseScriptStr(scheduleKey)

        if not premadeCronStr:
            startTime = datetime.fromtimestamp(timingInfo["startTime"] /
                                               1000.0)
            cronStr, suffix = self.convertTimingToCronStr(timingInfo,
                                                          startTime)
            if suffix:
                baseScript += " " + suffix
        else:
            if self.validateCronStr(premadeCronStr):
                cronStr = premadeCronStr
            else:
                # Error case: invalid cron str
                # TODO: handle this
                return False
        # Must export env variable as cronjobs run in mostly empty shells
        cronEntry = ("{cronExp} ({xlrDirEnv}; {pyPathEnv}; {executeRetina}) 2>&1 | "
                     "/usr/bin/logger -t ScheduledRetina_{schedKey}").format(
            cronExp = cronStr,
            xlrDirEnv = "export XLRDIR={}".format(os.environ["XLRDIR"]),
            pyPathEnv = "export PYTHONPATH={}".format(os.getenv("PYTHONPATH", "")),
            executeRetina = baseScript,
            schedKey = scheduleKey)
        return cronEntry

    def deleteAllFromCron(self):
        # DO NOT DO THIS.  TESTING ONLY.
        cronStr = self.cronTableTag
        logger.info("createSchedule: cronStr {}".format(cronStr))
        os.system("crontab -l | grep -v '" + self.cronTableTag + "' | crontab -")
