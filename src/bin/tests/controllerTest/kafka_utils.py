import io
import logging
import os
import subprocess
from datetime import datetime
from io import BytesIO
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from fastavro import parse_schema, reader, schema, writer
from faker import Faker
import uuid


class SingletonKafkaUtils():
    __instance = None

    @staticmethod
    def getInstance():
        if SingletonKafkaUtils.__instance is None:
            SingletonKafkaUtils.__instance = KafkaUtils()
        return SingletonKafkaUtils.__instance


class KafkaUtils():
    def __init__(self):
        self.KAFKA_BROKER = os.environ.get('KAFKA_BROKER', "{}:9092".format(os.getenv('HOSTIP', "localhost")))
        self.INPUT_TOPIC_1 = f'TEST_INTEGRATION_1_1_{uuid.uuid1()}'
        self.INPUT_TOPIC_2 = f'TEST_INTEGRATION_1_2_{uuid.uuid1()}'
        self.INPUT_TOPIC_3 = f'TEST_INTEGRATION_1_3_{uuid.uuid1()}'

        self.OUTPUT_TOPIC_1 = f'TEST_INTEGRATION_1_1_OUTPUT_{uuid.uuid1()}'
        self.OUTPUT_TOPIC_2 = f'TEST_INTEGRATION_1_2_OUTPUT_{uuid.uuid1()}'
        self.OUTPUT_TOPIC_3 = f'TEST_INTEGRATION_1_3_OUTPUT_{uuid.uuid1()}'

        self.REAL_MSG_TOPIC = f'TEST_INTEGRATION_REAL_MSG_{uuid.uuid1()}'

        self.CLI_STATIC_TOPIC = 'CLI_STATIC_TOPIC'

        self.SERVER_TOPIC_MAP = {
            self.KAFKA_BROKER: {
                self.INPUT_TOPIC_1: [{
                    'topic': self.INPUT_TOPIC_1,
                    'partition': 0
                }],
                self.INPUT_TOPIC_2: [{
                    'topic': self.INPUT_TOPIC_2,
                    'partition': 0
                }],
                self.INPUT_TOPIC_3: [{
                    'topic': self.INPUT_TOPIC_3,
                    'partition': 0
                }],
                self.OUTPUT_TOPIC_2: [{
                    'topic': self.OUTPUT_TOPIC_2,
                    'partition': 0
                }]
            }
        }

        self.KAFKA_CLUSTER_PROPS = {
            'topic_info': {
                self.INPUT_TOPIC_1: {
                    'bootstrap.servers': self.KAFKA_BROKER,
                    'group.id': f'CG1_{uuid.uuid1()}',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    },
                },
                self.INPUT_TOPIC_2: {
                    'bootstrap.servers': self.KAFKA_BROKER,
                    'group.id': f'CG1_{uuid.uuid1()}',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    },
                },
                self.INPUT_TOPIC_3: {
                    'bootstrap.servers': self.KAFKA_BROKER,
                    'group.id': f'CG1_{uuid.uuid1()}',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    },
                },
                self.OUTPUT_TOPIC_2: {
                    'bootstrap.servers': self.KAFKA_BROKER,
                    'group.id': f'CG1_{uuid.uuid1()}',
                    'enable.auto.commit': 'false',
                    'default.topic.config': {
                        'auto.offset.reset': 'smallest'
                    },
                }
            }
        }

    def push_to_kafka(self, producer, kafka_topic, parsed_schema, avro_rec):
        bytes_writer = io.BytesIO()
        writer(bytes_writer, parsed_schema, avro_rec)
        bytes_writer.seek(0)
        producer.produce(kafka_topic, bytes_writer.read())
        producer.flush()

    def publishRealMsgToKafka(self, max_count):
        logging.info('Started KAFKA Real Messages Publish')
        s = schema.load_schema(Path(__file__).parent.joinpath('schema.avsc'))
        parsed_schema = parse_schema(s)

        producer = Producer({'bootstrap.servers': self.KAFKA_BROKER})

        counter = 0
        fake = Faker()
        for j in range(0, max_count):
            avro_rec = {'group': 'group1', 'source': 'sourceTest'}
            headers = {}
            headers['header1'] = 'value1' + fake.uuid4()
            headers['header2'] = counter
            avro_rec['headers'] = headers
            avro_rec['data'] = {}
            data1 = {}
            data1['col1'] = counter
            data1['col2'] = fake.address()
            data1['col3'] = fake.name()
            data1['col4'] = fake.name()    # book.author
            data1['col5'] = fake.pyfloat(5, 5)
            data1['col6'] = fake.random_number()
            data1['col7'] = fake.company()
            data1['col8'] = fake.random_number() * 1.23 * counter / 4
            data1['col9'] = fake.name()    # app.name
            data1['col10'] = str(fake.pyfloat(0, 2))
            data1['col11'] = {}
            data1['col11']['timestamp'] = datetime.now()
            data1['col12'] = {}
            data1['col12']['date'] = datetime.today()
            avro_rec['data']['ds1'] = [data1]
            data2 = {}
            data2['d1'] = counter
            data2['d2'] = fake.name()    # dog().breed
            data2['d3'] = fake.name()    # artist().name()
            data2['d4'] = fake.name()    # gameOfThrones().character()
            data2['d5'] = fake.pyfloat(5, 5)
            data2['d6'] = fake.random_number()
            avro_rec['data']['ds2'] = [data2]
            counter += 1
            self.push_to_kafka(producer, self.REAL_MSG_TOPIC, parsed_schema,
                               [avro_rec])

        logging.info(
            'Done KAFKA Publish Real Messages. num:{}'.format(counter))

    def publishToKafka(self, max_count):
        logging.info('Started KAFKA Publish')
        s = schema.load_schema(Path(__file__).parent.joinpath('schema.avsc'))
        parsed_schema = parse_schema(s)

        with open('/netstore/controller/test_message', 'rb') as o:
            r = reader(io.BytesIO(o.read()))
            avro_rec = r.next()

        producer = Producer({'bootstrap.servers': self.KAFKA_BROKER})

        counter = 0
        for j in range(0, max_count):
            counter += 1
            for topic in [
                    self.INPUT_TOPIC_1, self.INPUT_TOPIC_2, self.INPUT_TOPIC_3
            ]:
                bytes_writer = io.BytesIO()
                writer(bytes_writer, parsed_schema, [avro_rec])
                bytes_writer.seek(0)
                producer.produce(topic, bytes_writer.read())

            # self.push_to_kafka(producer, self.INPUT_TOPIC_1, parsed_schema, [avro_rec])
            # self.push_to_kafka(producer, self.INPUT_TOPIC_2, parsed_schema, [avro_rec])
            # self.push_to_kafka(producer, self.INPUT_TOPIC_3, parsed_schema, [avro_rec])
        producer.flush()
        logging.info('Done KAFKA Publish. num:{}'.format(counter))

    def consumeFromKafka(self, topic):
        logging.info('Consuming from Kafka')
        conf = {
            'bootstrap.servers': self.KAFKA_BROKER,
            'group.id': 'CG5',
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'
        }
        c = Consumer(conf)

        def print_assignment(consumer, partitions):
            logging.info(f'Assignment:{partitions}')

        c.subscribe([topic], on_assign=print_assignment)
        try:
            msg = c.poll(timeout=10.0)
            if msg is None:
                return None
            if msg.error():
                raise KafkaException(msg.error())

            bytes_reader = BytesIO(msg.value())
            read = reader(bytes_reader)    # avro reader
            return list(read)
        finally:
            c.close()

    def cleanup(self):
        for topic in [
                self.INPUT_TOPIC_1, self.INPUT_TOPIC_2, self.INPUT_TOPIC_3,
                self.OUTPUT_TOPIC_1, self.OUTPUT_TOPIC_2, self.OUTPUT_TOPIC_3
        ]:
            try:
                args = [
                    os.path.dirname(Path(__file__)) +
                    '/kafka/bin/kafka-topics.sh', '--delete',
                    '--bootstrap-server', self.KAFKA_BROKER, '--topic', topic
                ]
                print('Running {}'.format(' '.join(args)))
                r = subprocess.run(
                    ' '.join(args),
                    shell=True,
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL)
                if r.returncode != 0:
                    print(f'Warning: Failed to delete topic: {topic}')

            except Exception as e:
                print(str(e))
                pass
