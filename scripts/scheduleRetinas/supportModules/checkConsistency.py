import os
import sys
import json
import subprocess
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
import logging

from socket import gethostname


# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Create Check consistency Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Check consistency - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Check consistency initialized; hostname:{}".format(gethostname()))


class ConsistencyChecker(object):
    def __init__(self, kvHandler=None, cronHandler=None):
        if kvHandler and cronHandler:
            self.kvHandler = kvHandler
            self.cronHandler = cronHandler
        else:
            self.kvHandler = ScheduleKVHandler()
            self.cronHandler = ScheduleCronHandler()

    def cleanup(self):
        self.kvHandler.cleanup()
        self.cronHandler.cleanup()

    def getCronInfo(self, scheduleKey=None):
        if scheduleKey:
            tmp = self.cronHandler.findScheduleInCron(scheduleKey)
            if not tmp:
                return []
            return [tmp]
        else:
            # Want info on all keys
            # Expect listings in cron are unique
            return self.cronHandler.findAllSchedulesInCron()

    def getKVInfo(self, scheduleKey=None):
        if scheduleKey:
            # Want info on one key
            tmp = self.kvHandler.getScheduleAndResults(scheduleKey)
            if not tmp:
                return []
            return [tmp]
        else:
            # Want info on all keys
            # Expect listings in cron are unique
            return self.kvHandler.getAllSchedulesAndResults()

    def validateTimingInfo(self, timingInfo):
        # TODO: Ensure that timingInfo is of the correct format
        return True

    def validateCreateObj(self, inObj):
        # TODO: require pyClient calls for checking param format,
        #       and checking if retname is valid
        # No duck typing here.  Don't want the error to be passed down to
        # user at the end of schedule run.
        scheduleKey = inObj.get("scheduleKey", None)
        if not scheduleKey:
            return False
        retName = inObj.get("retName", None)
        if not retName:
            return False
        params = inObj.get("params", None)
        if not params and not isinstance(params, list):
            # Empty list ok if no parameters
            return False
        options = inObj.get("options", None)
        if not options:
            if options.get("activeSession", None) is None:
                return False
            if not isinstance(options["activeSession"], bool):
                return False
            if options.get("newTableName", None) is None:
                return False
            if not isinstance(options["newTableName"], str):
                return False
        timingInfo = inObj.get("timingInfo", None)
        if not timingInfo:
            return False
        return validateTimingInfo

    def tryGetKVSchedKey(self, schedAndResult):
        scheduleMain = schedAndResult.get("scheduleMain", None)
        if scheduleMain:
            return scheduleMain.get("scheduleKey", None)
        return None


    def isKVValid(self, allKVInfo):
        keyCounts = {}
        entriesMalformed = []
        isValid = True
        for schedAndResult in allKVInfo:
            possibleKey = self.tryGetKVSchedKey(schedAndResult)
            if possibleKey:
                if (keyCounts.get(possibleKey, None) is None):
                    keyCounts[possibleKey] = 1

                else:
                    keyCounts[possibleKey] += 1
                    isValid = False
                entriesMalformed.append(False)
                continue
            else:
                isValid = False
                entriesMalformed.append(True)
#        keyCounts = {key : value for key, value in keyCounts.iteritems() if value > 1}
        return {
            "isValid": isValid,
            "keyCounts": keyCounts,
            "entriesMalformed": entriesMalformed
            }

    def isCronValid(self, allCronInfo):
        keyCounts = {}
        entriesMalformed = []
        isValid = True
        for cronInfo in allCronInfo:
            scheduleKey = cronInfo.get("scheduleKey", None)
            if scheduleKey:
                if (keyCounts.get(scheduleKey, None) is None):
                    keyCounts[scheduleKey] = 1
                else:
                    keyCounts[scheduleKey] += 1
                    isValid = False
                entriesMalformed.append(False)
                continue
            isValid = False
            entriesMalformed.append(True)
#        keyCounts = {key : value for key, value in keyCounts.iteritems()
#                                if value > 1}
        return {
            "isValid": isValid,
            "keyCounts": keyCounts,
            "entriesMalformed": entriesMalformed
            }


    def consistencyCheckHelper(self, scheduleCronInfo, scheduleKVInfo):
        cronValidityObj = self.isCronValid(scheduleCronInfo)
        kvValidityObj = self.isKVValid(scheduleKVInfo)
        cronValidity = cronValidityObj["isValid"]
        kvValidity = kvValidityObj["isValid"]
        cronKeyCount = cronValidityObj["keyCounts"]
        kvKeyCount = kvValidityObj["keyCounts"]

        assumptionsValidated = {
            "kvInternalValid" : True,
            "cronInternalValid" : True,
            "cronSupersetKV" : True,
            "kvSupersetCron" : True
        }

        if not cronValidity:
            # TODO: error case, individual stores are internally inconsistent
            assumptionsValidated["cronInternalValid"] = False

        if not kvValidity:
            assumptionsValidated["kvInternalValid"] = False

        for scheduleKey in list(kvKeyCount.keys()):
            if not cronKeyCount.get(scheduleKey, None):
                assumptionsValidated["cronSupersetKV"] = False

        for scheduleKey in list(cronKeyCount.keys()):
            if not kvKeyCount.get(scheduleKey, None):
                assumptionsValidated["kvSupersetCron"] = False

        return assumptionsValidated

    def consistencyRepairHelper(self, scheduleCronInfo, scheduleKVInfo):
        assumptionsValidated = {
            "kvInternalValid" : True,
            "cronInternalValid" : True,
            "cronSupersetKV" : True,
            "kvSupersetCron" : True
        }
        kvValidityObj = self.isKVValid(scheduleKVInfo)
        if not kvValidityObj["isValid"]:
            # kv is internally invalid
            if any([val != 1 for val in list(kvValidityObj["keyCounts"].values())]):
                # Multiple entries case
                updatedKeys = self.kvHandler.deleteOldMainSchedules()
            elif any(kvValidityObj["entriesMalformed"]):
                # Malformed kv entry
                # TODO: handle this
                # Annoying because malformed in this case means entry has no
                # scheduleKey, but entries are indexed in helpers by key
                # not by ordering
                pass
            else:
                # Unknown error
                pass
            assumptionsValidated["kvInternalValid"] = False

        cronValidityObj = self.isCronValid(scheduleCronInfo)
        if not cronValidityObj["isValid"]:
            # cron is internally invalid
            # POLICY: pick the more recently updated one and validate
            if any([val != 1 for val in list(cronValidityObj["keyCounts"].values())]):
                for scheduleKey in list(cronValidityObj["keyCounts"].keys()):
                    self.cronHandler.removeScheduleFromCron(scheduleKey)
                    kvObj = self.kvHandler.getSchedule(scheduleKey)
                    self.cronHandler.addScheduleObjToCron(kvObj)

            elif any(cronValidityObj["entriesMalformed"]):
                # Malformed cron entry
                # TODO: handle this
                # Annoying because malformed in this case means entry has no
                # scheduleKey, but entries are indexed in helpers by key
                # not by ordering
                pass
            else:
                # TODO: handle this
                # Unknown error
                pass
            assumptionsValidated["cronInternalValid"] = False

        for scheduleKey in list(kvValidityObj["keyCounts"].keys()):
            # kv has a key that cron does not
            if not scheduleKey in list(cronValidityObj["keyCounts"].keys()):
                kvObj = self.kvHandler.getSchedule(scheduleKey)
                self.cronHandler.addScheduleObjToCron(kvObj)

            assumptionsValidated["cronSupersetKV"] = False

        for scheduleKey in list(cronValidityObj["keyCounts"].keys()):
            # cron has a key that kv does not
            if not scheduleKey in list(kvValidityObj["keyCounts"].keys()):
                self.cronHandler.removeScheduleFromCron(scheduleKey)

            assumptionsValidated["kvSupersetCron"] = False

        return assumptionsValidated

    # TODO: There is a lock around the kv operations.  Need to wrap
    # the kvlock around both the kv and cron operations so that kv is always
    # in lockstep with cron, and inconsistencies aren't a valid state
    # Once that is done we can do the consistency check
    def consistencyCheck(self, scheduleKey=None):
        scheduleCronInfos = self.getCronInfo(scheduleKey)
        scheduleKVInfos = self.getKVInfo(scheduleKey)
        return self.consistencyCheckHelper(scheduleCronInfos, scheduleKVInfos)

    def consistencyRepair(self, scheduleKey=None):
        scheduleCronInfos = self.getCronInfo(scheduleKey)
        scheduleKVInfos = self.getKVInfo(scheduleKey)
        return self.consistencyRepairHelper(scheduleCronInfos, scheduleKVInfos)
