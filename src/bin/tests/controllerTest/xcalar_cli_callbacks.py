from datetime import datetime
from test_kafka_dist import Distributer
from kafka_utils import SingletonKafkaUtils
# import uuid

# Call-back functions returning literals
kafka_utils = SingletonKafkaUtils.getInstance()


def getCurrentTimestamp():
    return datetime.now().timestamp() * 1000


def getInitBatchId():
    return 1


def runtimeParamsCallBack4TestRefiner(params):
    return {'max8': 10, 'system': 'test', 'test': 100}


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
    return ds1_cnt + ds2_cnt > 100


def getXpuPartitionMap(xpuCount,
                       distributer_props,
                       universe_id,
                       materialization_times=None):
    dist = Distributer(xpuCount, distributer_props)
    dist.server_topic_map = {
        kafka_utils.KAFKA_BROKER: {
            kafka_utils.CLI_STATIC_TOPIC: [{
                'topic': kafka_utils.CLI_STATIC_TOPIC,
                'partition': 0
            }]
        }
    }
    dist.kafka_cluster_props = {
        'topic_info': {
            kafka_utils.CLI_STATIC_TOPIC: {
                'bootstrap.servers': kafka_utils.KAFKA_BROKER,
                'group.id': f'CLI_CONSUMER_GROUP_{universe_id}',
                'enable.auto.commit': 'false',
                'default.topic.config': {
                    'auto.offset.reset': 'smallest'
                },
            }
        }
    }

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
    if len(watermark.batchInfo) >= 100:
        return True
    else:
        return False
