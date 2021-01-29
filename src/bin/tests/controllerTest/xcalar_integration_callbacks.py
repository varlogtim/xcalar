from datetime import datetime
from test_kafka_dist import Distributer

# Call-back functions returning literals


def getCurrentTimestamp():
    return datetime.now().timestamp() * 1000


def getInitBatchId():
    return 1


def runtimeParamsCallBack4TestRefiner(params, watermark=None):
    batches = watermark.batchInfo.keys()
    latest_batch_id = max([int(b) for b in batches])
    return {'max8': 10, 'system': 'test', 'test': 100, 'batch_id': latest_batch_id}


def runtimeParamsCallBack4TestRefiner2(params):
    return {'param1': 'value1'}


def decisionCallBack4TestRefiner(app_name, watermark):
    lowBatch = watermark.getLowBatchId(app_name)
    return lowBatch < 99999


def decisionCallBack4TestRefinerBL(app_name, watermark):
    lowBatch = watermark.getLowBatchId(app_name)
    bb = watermark.batchInfo
    ds1_cnt = sum([bb[b].get('ds1', 0) for b in bb if int(b) >= lowBatch])
    ds2_cnt = sum([bb[b].get('ds2', 0) for b in bb if int(b) >= lowBatch])
    if lowBatch in bb.keys():
        now_ts = datetime.now().timestamp()
        last_ts = bb[lowBatch]['$timestamp']/1000
        time_since_last_run = now_ts - last_ts
        return ds1_cnt + ds2_cnt > 100 or time_since_last_run > 1000
    else:
        return ds1_cnt + ds2_cnt > 100


def getXpuPartitionMap(xpuCount,
                       distributer_props,
                       universe_id,
                       materialization_times=None):
    dist = Distributer(xpuCount, distributer_props)

    def xpuPartitionMap(batch_id, offsets):
        dist.distribute()
        rv = dist.updateOffsets(batch_id, offsets)
        return rv

    return xpuPartitionMap


def isTimeToSnapshot(watermark, analyzer):
    # t = analyzer.executeSql('SELECT * FROM $info')
    # print(analyzer.toPandasDF(t).head(1000))
    return watermark.batch_id % 5 == 0


def loadRefDataBarrier(watermark, lowBatch):
    return len(watermark.batchInfo.keys()) == 1

def isTimetoPurge(watermark):
    if max(watermark.batchInfo.keys()) >= 5:
        return True
    else:
        return False

