import fastavro
from fastavro import writer, reader, parse_schema
import math
import logging
import os
import codecs
import csv
import time
import datetime
import json
from json import dumps
from confluent_kafka import Producer, TopicPartition, KafkaError
import io
from zipfile import ZipFile
import xcalar.container.driver.base as driver
import xcalar.container.cluster

logger = logging.getLogger('xcalar')
Utf8Encoder = codecs.getwriter('utf-8')
def push_to_kafka(producer, kafka_topic, parsed_schema, avro_rec, partition):
    bytes_writer = io.BytesIO()
    writer(bytes_writer, parsed_schema, avro_rec)
    bytes_writer.seek(0)
    producer.produce(kafka_topic, bytes_writer.read(), partition=int(partition), on_delivery=acked)
    # producer.poll(0)
    # producer.flush()
def acked(err, msg):
    if err is not None:
        logger.info("Failed to deliver message: {}".format(err))
    else:
        logger.info("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))
@driver.register_export_driver(name='${exportDriverName}', is_builtin=True)
@driver.param(
    name='target',
    type=driver.TARGET,
    desc='Select a connector for the export.')
@driver.param(
    name='avsc_fname',
    type=driver.STRING,
    desc='Full File name <path/filename> of the avro schema')
@driver.param(
    name='kafka_params',
    type=driver.STRING,
    desc=" eg :{\"dataset_name\" : \"ds1\",\"batch_size\": 1000}")
@driver.param(
    name='kafka_topic',
    type=driver.STRING,
    desc='Kafka Topic Name')
@driver.param(
    name='partition',
    type=driver.STRING,
    desc='Kafka Partition')
# { "time_stamp": "DfString", "value": "DfString", "company": "DfString", "metric": "DfString", "cluster":"DfString","node": "DfString"}
@driver.param(
    name='max_headers',
    type=driver.STRING,
    desc=" eg :{\"qtm_cdc_time\" : \"source_extract_from_ts\"}")
def driver(table,
           target,
           avsc_fname,
           kafka_params,
           kafka_topic,
           partition,
           max_headers):
    """Export data to many AVRO files in the specified target. The number of files
    is determined at runtime, but is correlated with the number of nodes in the
    Xcalar cluster and the number of cores on those nodes.
    """

    os.environ["KRB5CCNAME"] = "/tmp/XCALAR.KAFKA"

    kafka_server_json = json.loads(kafka_params)
    # number of recs in a single avro message
    MAX_RECS_IN_AVRO = kafka_server_json['batch_size']
    # Initiaize Kafka Producer
    producer = Producer(json.loads('${kafkaProducerProps}'))
    # get xcalar_schema
    columns = [c['columnName'] for c in table.columns()]
    xcalar_schema = {
        col.name: col.type
        for col in table._meta.columns if col.name in columns
    }
    max_headers_list = json.loads(max_headers)
    # serialize timestamp
    # datetime.datetime.max.timestamp()
    # 253402329600.0
    def avro_timestamp_serialize(field_name: str, ts: int) -> dict:
        record = {}
        record[field_name] = {
            'timestamp': ts
        }    # for actual timestamp: reset to ts*1000
        return record
    # serialize date
    def avro_date_serialize(field_name: str, date: int) -> dict:
        d_int = math.floor(date / 1000 / 60 / 60 / 24)
        inner_record = {}
        inner_record['date'] = d_int
        record = {}
        record[field_name] = inner_record
        return record
    # serialize decimal
    def avro_decimal_serialize(field_name: str, dec_str: str) -> dict:
        inner_record = {}
        inner_record['decimal'] = dec_str
        record = {}
        record[field_name] = inner_record
        return record
        # load xcalar schema
    # load avsc schema
    schema = fastavro.schema.load_schema(avsc_fname)
    # parse schema
    parsed_schema = parse_schema(schema)
    dataset_name = kafka_server_json['dataset_name']
    avro_data = {
        'group': 'group1',
        'source': 'source1',
        'headers': {
            'header1': 'value1',
            'header2': 1
        },
        'data': {
            dataset_name: [kafka_server_json['dataset_name']]
        }
    }
    header_fields = max_headers_list
    max_headers_dict = {k:0 for k in header_fields}
    if partition.isnumeric():
        recs = []
        consumed_rows = 0
        for row in table.partitioned_rows():
            for field in row:
                if field in header_fields:
                    max_headers_dict[field] = max(row[field],max_headers_dict[field])

                if xcalar_schema[field] == 'Timestamp':
                    new_field = avro_timestamp_serialize(field, row[field])
                    row[field] = new_field[field]
                elif xcalar_schema[field] == 'Date':
                    new_field = avro_date_serialize(field, row[field])
                    row[field] = new_field[field]
                elif xcalar_schema[field] == 'Decimal':
                    new_field = avro_decimal_serialize(field, row[field])
                    row[field] = new_field[field]
            recs.append(row)
            consumed_rows = consumed_rows + 1
            if (consumed_rows == MAX_RECS_IN_AVRO):
                # logger.info('Consumed {} messages. Now publishing to kafka.'.format(MAX_RECS_IN_AVRO))
                avro_data['data'][dataset_name] = recs
                push_to_kafka(producer, kafka_topic, parsed_schema, [avro_data], partition)
                # logger.info('Published {} messages to kafka.'.format(MAX_RECS_IN_AVRO))
                recs = []
                consumed_rows = 0
                avro_data['data'][dataset_name] = []    # clear avro struct
        if len(recs) > 0:    # dump the rest of the records
            avro_data['data'][dataset_name] = recs
            avro_data['headers'] = {**avro_data['headers'], **max_headers_dict}
            push_to_kafka(producer, kafka_topic, parsed_schema, [avro_data], partition)
            consumed_rows = 0
        # Finally, flush all the records
        producer.flush()
    else:
        partititions = {}
        for row in table.partitioned_rows():
            partition_num = -1
            for field in row:
                if field in header_fields:
                    max_headers_dict[field] = max(row[field],max_headers_dict[field])

                if field == partition:
                    partition_num = int(row[field])
                    partititions[partition_num] = partititions.get(partition_num,[])
                elif xcalar_schema[field] == 'Timestamp':
                    new_field = avro_timestamp_serialize(field, row[field])
                    row[field] = new_field[field]
                elif xcalar_schema[field] == 'Date':
                    new_field = avro_date_serialize(field, row[field])
                    row[field] = new_field[field]
                elif xcalar_schema[field] == 'Decimal':
                    new_field = avro_decimal_serialize(field, row[field])
                    row[field] = new_field[field]
            partititions[partition_num].append(row)
            if (len(partititions[partition_num]) >= MAX_RECS_IN_AVRO):
                avro_data['data'][dataset_name] = partititions[partition_num]
                # logger.info(f'EXPORT to partition {partition_num}: {MAX_RECS_IN_AVRO} rows.')
                avro_data['headers'] = {**avro_data['headers'], **max_headers_dict}
                push_to_kafka(producer, kafka_topic, parsed_schema, [avro_data], partition_num)
                partititions[partition_num] = []
                avro_data['data'][dataset_name] = []    # clear avro struct
        # dump the rest of the records
        for partition_num, recs in partititions.items():
            if len(recs) > 0:    # dump the rest of the records
                avro_data['data'][dataset_name] = recs
                avro_data['headers'] = {**avro_data['headers'], **max_headers_dict}
                push_to_kafka(producer, kafka_topic, parsed_schema, [avro_data], partition_num)
                # logger.info(f'EXPORT to partition {partition_num}: {len(recs)} rows.')
                consumed_rows = 0
        # Finally, flush all the records
        producer.flush()