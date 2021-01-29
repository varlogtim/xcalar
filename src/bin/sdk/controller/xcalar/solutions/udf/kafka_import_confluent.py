import datetime
import logging
import time

from io import BytesIO
import xcalar.container.context as ctx
from confluent_kafka import Consumer, TopicPartition, KafkaError
from fastavro import reader
import json
from xcalar.external.client import Client

import os

client = Client(bypass_proxy=True)
kvstore = client.global_kvstore()
logger = logging.getLogger('xcalar')
MILLION = 1000000
MAX_NONE_MESSAGES = 20
POLL_TIMEOUT = 1

def parse_kafka_topic(fullPath, inStream, key):
    os.environ["KRB5CCNAME"] = "/tmp/XCALAR.KAFKA"
    params = json.loads(kvstore.lookup(key))
    opts = params['opts']
    universe = params['universe']
    try:
        # Check if this xpu is assigned to any kafa topic
        current_xpu_id = str(ctx.get_xpu_id())
        # xpu_r = xpu % (ctx.get_xpu_cluster_size() - 1)
        if current_xpu_id not in opts['metadataInfo']:
            return
        # Get kafka connection params
        kafkaOptions = opts['metadataInfo'][current_xpu_id]
        # Generate topic / partition list for this xpu
        topic = kafkaOptions['topic']
        topicPartitionList = []
        # shared info for each record
        record = {
            'batch_id': opts['batch_id'],
            'xpu_id': current_xpu_id,
            'topic': topic,
            'partition': -1,
            'timestamp': datetime.datetime.now().timestamp() * 1000
        }
        for part_key in kafkaOptions['partitionInfo']:
            partition_info = kafkaOptions['partitionInfo'][part_key]
            topicPartitionList.append(
                TopicPartition(topic, int(part_key), partition_info['min']))
        messages_consumed = 0
        kafkaOptions['kafka_props']['debug'] = 'consumer'
        consumer = Consumer(kafkaOptions['kafka_props'])
        consumer.assign(topicPartitionList)
        numMessagesToBeConsumed = opts['maxIter']
        # To report kafka messages with zero avro records in it
        otherDatasets = set()
        # pull kafka messages with 10 sec time out until max iter is reached
        non_messages = 0
        while messages_consumed < numMessagesToBeConsumed:
            record['partition'] = -1
            msg = consumer.poll(POLL_TIMEOUT)
            # if brokers are down msg is null
            if msg is None:
                if non_messages > MAX_NONE_MESSAGES:
                    record['dataset'] = 'info'
                    record['status'] = 'info'
                    record['offset'] = -1
                    record['msg'] = 'No records ingested'
                    yield record
                    break
                else:
                    non_messages += 1
                    continue
            non_messages = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise (Exception(msg.error()))

            # Avro business begins here, convert kafka msg to binary
            bytes_reader = BytesIO(msg.value())
            records = reader(bytes_reader)    # avro reader
            for rec in records:    # iterate each avro record , look for data field
                if 'data' in rec:
                    for dsName in rec[
                            'data']:    # map<dataset, list<map<string, object>>>
                        record['partition'] = msg.partition()
                        if dsName in universe:
                            universeTableName = universe[dsName]
                            record['dataset'] = universeTableName
                            record['offset'] = msg.offset()
                            for row in rec['data'][dsName]:
                                if type(row) == dict:
                                    for k in row:
                                        if type(row[k]) == dict:
                                            val = list(row[k].values())[0]
                                            if type(val) == datetime.datetime:
                                                row[k] = val.timestamp(
                                                ) * MILLION
                                            elif type(val) == datetime.date:
                                                row[k] = time.mktime(
                                                    val.timetuple()) * MILLION
                                            else:
                                                row[k] = type(val)
                                    record['record'] = row
                                yield record
                        else:
                            otherDatasets.add(dsName)
            if bytes_reader:
                bytes_reader.close()
            messages_consumed += 1
        if len(otherDatasets) > 0:
            record['dataset'] = 'info'
            record['status'] = 'info'
            record['offset'] = -1
            record['msg'] = 'Datasets not in universe: {}'.format(
                otherDatasets)
            yield record

    except Exception as e:
        record['dataset'] = 'info'
        record['status'] = 'error'
        record['offset'] = -1
        record['msg'] = str(e)
        yield record

    finally:
        try:
            consumer.close()
        except Exception:
            pass
