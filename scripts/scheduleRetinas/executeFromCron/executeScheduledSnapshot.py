import random
from datetime import datetime

from xcalar.external.client import Client
from xcalar.compute.coretypes.Status.ttypes import *
import executeSchedule
import logging
from socket import gethostname

# Dynamic Constants
hostname = gethostname()

# Set up logging
logger = logging.getLogger('Execute Scheduled Snapshot Logger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # Logging is not multi-process safe, so stick to stdout
    logHandler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
            '%(asctime)s - Execute Scheduled Snapshot - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)

    logger.debug("Execute Scheduled Snapshot initialized; hostname:{}".format(gethostname()))


class SnapshotScheduler(executeSchedule.Scheduler):
    def __init__(self, scheduleName, scheduleTag, biweekly):
        super().__init__(scheduleName, scheduleTag, biweekly)
        inputInfo = self.kvHandler.getScheduleArguments(self.scheduleName)
        self.startTime = datetime.fromtimestamp(inputInfo["timingInfo"]["startTime"] / 1000.0)
        self.options = inputInfo["options"]

    def execute(self):
        client = Client(bypass_proxy=True)
        resultObj = {
            "randomNum" : random.uniform(1, 100),
            "startTime" : self.curTimeInMS()
        }
        self.kvHandler.updateScheduleResults(self.scheduleName, resultObj)

        logger.info("Snapshot scheduler {}".format(self.scheduleName))

        try:
            # Kick snapshot App now
            client.publish_table_snap_trigger(publish_table_name_pattern="*", cronJob=True)
            resultObj["status"] = StatusT.StatusOk
        except Exception as e:
            resultObj["status"] = str(e)
            print({"status": str(e), "error": e})

        resultObj["endTime"] = self.curTimeInMS()
        self.kvHandler.updateScheduleResults(self.scheduleName, resultObj)
        return resultObj
