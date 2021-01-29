# PLEASE TAKE NOTE:

# UDFs can only support
# return values of
# type String.

# Function names that
# start with __ are
# considered private
# functions and will not
# be directly invokable.

# binary stream to kafka topic
import fastavro
from fastavro import writer, parse_schema
import math
import logging
import codecs
import json
from confluent_kafka import Producer, KafkaError, KafkaException
import io
import xcalar.container.driver.base as driver
from confluent_kafka.admin import AdminClient, NewTopic
import xcalar.container.context as ctx

logger = logging.getLogger('xcalar')
Utf8Encoder = codecs.getwriter('utf-8')

NUM_PARTITIONS = 10


def create_topic(kafka_props, kafka_topic):
    a = AdminClient(kafka_props)
    new_topic = NewTopic(
        kafka_topic, num_partitions=NUM_PARTITIONS, replication_factor=1)
    try:
        result = a.create_topics([new_topic])
        result[kafka_topic].result(timeout=300)
    except KafkaException as ke:
        if ke.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise ke


def push_to_kafka(producer, kafka_topic, parsed_schema, avro_rec, partition):
    bytes_writer = io.BytesIO()
    writer(bytes_writer, parsed_schema, avro_rec)
    bytes_writer.seek(0)
    producer.produce(kafka_topic, bytes_writer.read(), 'test_key', partition)


@driver.register_export_driver(name='data_gen_kafka_export', is_builtin=True)
@driver.param(
    name='target',
    type=driver.TARGET,
    desc='Select a connector for the export.')
@driver.param(
    name='kafka_server',
    type=driver.STRING,
    desc='host:port of kafka broker server')
@driver.param(
    name='avsc_fname',
    type=driver.STRING,
    desc='Full File name <path/filename> of the avro schema')
@driver.param(
    name='kafka_params',
    type=driver.STRING,
    desc=" eg :{\"dataset_name\" : \"ds1\",\"batch_size\": 1000}")
@driver.param(name='kafka_topic', type=driver.STRING, desc='Kafka Topic Name')
# { "time_stamp": "DfString", "value": "DfString", "company": "DfString", "metric": "DfString", "cluster":"DfString","node": "DfString"}
def driver(table, target, kafka_server, avsc_fname, kafka_params, kafka_topic):
    """Export data to many AVRO files in the specified target. The number of files
    is determined at runtime, but is correlated with the number of nodes in the
    Xcalar cluster and the number of cores on those nodes.
    """
    kafka_server_json = json.loads(kafka_params)
    # number of recs in a single avro message
    MAX_RECS_IN_AVRO = kafka_server_json['batch_size']
    # Initiaize Kafka Producer
    kafka_producer_props = {
        "bootstrap.servers": kafka_server,
    # assuming max record size is xdb default page size 128KB
        "message.max.bytes": MAX_RECS_IN_AVRO * 128 * 1024
    }
    producer = Producer(kafka_producer_props)
    # get xcalar_schema
    columns = [c['columnName'] for c in table.columns()]
    xcalar_schema = {
        col.name: col.type
        for col in table._meta.columns if col.name in columns
    }

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
    recs = []
    consumed_rows = 0
    counter = ctx.get_xpu_id()
    for row in table.partitioned_rows():
        for field in row:
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
            logger.info('Consumed {} messages. Now publishing to kafka.'.
                        format(MAX_RECS_IN_AVRO))
            avro_data['data'][dataset_name] = recs
            create_topic({'bootstrap.servers': kafka_server}, kafka_topic)
            partition = counter % NUM_PARTITIONS
            push_to_kafka(producer, kafka_topic, parsed_schema, [avro_data],
                          partition)
            logger.info(
                f'Published {consumed_rows} messages to {kafka_topic} kafka, partition: {partition}'
            )
            recs = []
            consumed_rows = 0
            avro_data['data'][dataset_name] = []    # clear avro struct
            counter = counter + 1

    if len(recs) > 0:    # dump the rest of the records
        logger.info('Consumed {} messages. Now publishing to kafka.'.format(
            len(recs)))
        avro_data['data'][dataset_name] = recs
        push_to_kafka(producer, kafka_topic, parsed_schema, [avro_data],
                      counter % NUM_PARTITIONS)
        logger.info('Published {} messages to kafka.'.format(len(recs)))
        consumed_rows = 0

    # Finally, flush all the records
    producer.flush()
