import calendar
from datetime import datetime, date
import dateutil
import time
import multiprocessing

from rig_kafka_dist import Distributer


# Call-back functions returning literals
def getCurrentTimestamp():
    return datetime.now().timestamp() * 1000


def getInitBatchId():
    return 1


# If any kafka dataset has data, run it.
# Todo: get list of datasets to check from Ravi
def decisionCallBack4TestRefiner(app_name, watermark):
    """Method that xcalar calls for deciding if we should run the refiner"""
    # XXX just for testing purposes
    # This make refiner to run always in every batch
    return True
    # XXX end

    lowBatch = watermark.getLowBatchId(app_name)
    bb = watermark.batchInfo
    datasetList = [
        'DailyAccrualTBV2', 'BDADailyBalAccrlTBV2', 'BDACustIntTBV2',
        'GSEntlPosnTB', 'GSEntlInflightTBV2', 'NFidsAccrInfoTB',
        'GASSAnnounceTB', 'GSGassEresponseTBV2', 'GASSAnnChoiceTB',
        'EDSAnnounceTB', 'GSUnitEqAccrlTBV2', 'GainLossSTB', 'PositionsSTB',
        'PWM_Prices', 'PWM_Unit_FI_Accruals', 'Currency_FX_Rate',
        'Taxlots_Cost'
    ]
    for ds in datasetList:
        if sum([bb[b].get(ds, 0) for b in bb if int(b) >= lowBatch]):
            return True


# Todo: Get logic for this from Ravi
def getRuntimeParamsForVac(params, watermark=None):
    """Method that xcalar calls for supplying run time params for refiner"""
    return {
        'currentBusinessDate': 1568779200000000,    # get today's date
        'InfinityTimeSTamp': 253000000000000000,
        'currentTimestamp': 1568779200000000,    # get now
        'maxAsOfDate': getMaxAsOfDateInMicros(
        ),    # month end of (current business date - 25 months)
        'source_system_name': "PC6",
        'sourceSystemName': "PC6"
    }


def getMaxAsOfDateInMicros():
    dt = date.today()
    relativeDate = dt - dateutil.relativedelta.relativedelta(months=25)
    monthEndDay = calendar.monthrange(relativeDate.year, relativeDate.month)[1]
    monthEndDate = date(relativeDate.year, relativeDate.month, monthEndDay)
    monthEndDateInSeconds = int(time.mktime(monthEndDate.timetuple()))
    return monthEndDateInSeconds * 1000000


def getCurrentTimestampInMicro():
    return int(datetime.now().timestamp() * 1000000)


def getTodayDateInEpochMicro():
    return getTodayDateInEpochSeconds() * 1000000


def getTodayDateInEpochSeconds():
    return int(time.mktime(date.today().timetuple()))


# TODO: Check with Dave
def isTimeToSnapshot(watermark, analyzer):
    return False    # watermark.batch_id%10 == 0


def getXpuPartitionMap(xpuCount, distributorProps, universe_id,
                       materializationTimes):
    """Method that xcalar calls for distributing kafka topic partitions to xpu"""
    minmattime = getMinMaterializationTime(materializationTimes)
    print(f'Materialization Times {materializationTimes}')
    print(f'Minimum Materialization Time {minmattime}')
    xpuCount = multiprocessing.cpu_count()
    dist = Distributer(xpuCount, distributorProps)

    def xpuPartitionMap(batch_id, offsets):
        dist.distribute()
        rv = dist.updateOffsets(batch_id, offsets)
        return rv

    return xpuPartitionMap


def getMinMaterializationTime(materializationTimes):
    minmattime = -1
    for mattim in (materializationTimes.values()):
        if (minmattime <= 0):
            minmattime = mattim
        elif (mattim < minmattime):
            minmattime = mattim
    minmattime = int(minmattime) - 100
    minmattime = 1582164000
    return minmattime

def isTimetoPurge(watermark):
        return False
