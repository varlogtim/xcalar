import asyncio
import json
import logging
import os
import random
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path

import pytest
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from xcalar.container.kafka.kafka_app_runner import runKafkaApp
from xcalar.container.kafka.kafka_import_udf import parse_kafka_topic
from xcalar.container.kafka.offsets_manager import OffsetsManager
from xcalar.container.kafka.partition_manager import PartitionManager
from xcalar.container.kafka.topic_handler import TopicHandler, TopicInfo
from xcalar.container.kafka.xcalar_client_utils import get_or_create_session
from xcalar.container.kafka.dataflow_invoker import DataflowInvoker

from xcalar.external.app import App
from xcalar.external.client import Client
from xcalar.external.kvstore import KvStore
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.table import Table

NUM_PARTITIONS = 10
TIMEOUT_IN_SECS = 60
BATCH_SIZE = 50
#set a timeout to make sure the topic is created
CREATE_TOPIC_TIMEOUT_SECS = 100

@pytest.fixture
def setup(client):
    session_name = f'test_kafka_{datetime.now().strftime("%s")}'
    xcalarApi = XcalarApi()
    session = get_or_create_session(client, session_name)
    xcalarApi.setSession(session)
    data_targets = list(map(lambda dt: dt.name, client.list_data_targets()))
    if 'kafka-base' in data_targets:
        client.get_data_target('kafka-base').delete()
    client.add_data_target(target_name="kafka-base", target_type_id="memory")
    yield (xcalarApi, client, session)

    try:
        for table in session.list_tables():
            table.drop(delete_completely=True)
        session.destroy()
    except Exception:
        pass


def wait_for_future(fut):
    # If timeout is None, block until the future completes.
    asyncio.wait_for(fut, timeout=None)
    print(fut)
    if fut.exception():
        raise fut.exception()


@pytest.fixture()
def setup_kafka(setup):
    (xcalarApi, xcalarClient, session) = setup
    topics = [
        f'test_1_{datetime.now().strftime("%s")}',
        f'test_2_{datetime.now().strftime("%s")}',
        f'test_3_{datetime.now().strftime("%s")}'
    ]

    partition_offsets = {}
    for i in range(0, NUM_PARTITIONS):
        partition_offsets[i] = 0

    custom_udf_name = 'simple_message_parser'
    udf_file = os.path.dirname(Path(__file__)) + f'/{custom_udf_name}.py'

    kafka_app_dir = tempfile.TemporaryDirectory()
    shutil.copyfile(udf_file, f'{kafka_app_dir.name}/simple_message_parser.py')

    kafka_configs = []
    new_topics = []
    producers = []
    for topic_name in topics:
        print(f'Creating topic {topic_name}')
        kafka_config = {
            'topic': topic_name,
            'parser_udf_name': custom_udf_name,
            'parser_udf_path': f'{kafka_app_dir.name}/{custom_udf_name}.py',
            'batch_size': BATCH_SIZE,
            'poll_timeout_in_secs': TIMEOUT_IN_SECS,
            'batch_timeout_in_secs': TIMEOUT_IN_SECS,
            'topic_props': {
                'group.id': f'CG1_{uuid.uuid1()}',
                'bootstrap.servers': "{}:9092".format(os.getenv('HOSTIP', "localhost")),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false',
            },
            'partition_offsets': partition_offsets
        }
        kafka_configs.append(kafka_config)
        new_topics.append(
            NewTopic(
                topic_name,
                num_partitions=len(kafka_config['partition_offsets']),
                replication_factor=1))
        producers.append(Producer(kafka_config['topic_props']))

    a = AdminClient(kafka_config['topic_props'])
    try:
        result = a.create_topics(new_topics)
        for topic_name in topics:
            wait_for_future(result[topic_name])
            #waiting for creating topic until <create_time>
            wait_until = time.time() + CREATE_TOPIC_TIMEOUT_SECS
            while time.time() < wait_until and topic_name not in a.list_topics().topics:
                print(f'waiting for creating topic : {topic_name}')
                time.sleep(1)
            assert topic_name in a.list_topics().topics

    except KafkaException as ke:
        if ke.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise ke

    yield (kafka_configs, producers)

    # cleanup
    for topic_name in topics:
        result = a.delete_topics([topic_name])
        wait_for_future(result[topic_name])


@pytest.fixture()
def configuration(setup, setup_kafka):
    (xcalarApi, xcalarClient, session) = setup
    (kafka_configs, producers) = setup_kafka
    result = []
    for kafka_config in kafka_configs:
        result.append({
            'session_name': session.name,
            'kafka_config': kafka_config
        })
    yield result


# Used only during development
@pytest.mark.skip
@pytest.mark.nocontainer
def test_publish(capsys, setup_kafka):
    with capsys.disabled():
        (kafka_configs, producers) = setup_kafka
        from xcalar.container.kafka.kafka_metadata import KafkaMetadata
        mt = KafkaMetadata(consumer_props=kafka_configs[0]['topic_props'])
        print(mt.get_partitions(kafka_configs[0]['topic']))
        publish_messages(kafka_configs, producers, 2)


def test_partition_manager(capsys):
    with capsys.disabled():
        pm = PartitionManager(logging.getLogger('xcalar'))
        xpu_partition_map = pm.xpu_partition_map(
            3, [0, 4, 8], [4, 4, 4], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        assert {
            0: [0],
            1: [1],
            2: [2],
            3: [3],
            4: [4],
            5: [5],
            6: [6],
            7: [7],
            8: [8],
            9: [9]
        } == xpu_partition_map
        xpu_partition_map = pm.xpu_partition_map(
            3, [100, 104, 108], [4, 4, 4], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        assert {
            100: [0],
            101: [1],
            102: [2],
            103: [3],
            104: [4],
            105: [5],
            106: [6],
            107: [7],
            108: [8],
            109: [9]
        } == xpu_partition_map

@pytest.mark.nocontainer
def test_offsets_manager_read_partition_offset_map(capsys, setup):
    (xcalarApi, xcalarClient, session) = setup
    with capsys.disabled():
        scope = get_or_create_session(xcalarClient, session.name).scope
        om = OffsetsManager(xcalarClient, scope)
        topic = f'test_kafka_{uuid.uuid1()}'
        partitions = [0, 1, 2, 3, 4, 5]
        kv_store = KvStore(xcalarClient, scope)
        for partition in partitions:
            key = om.get_new_partition_offset_key(topic, partition)
            kv_store.add_or_replace(key, str(10), True)
        assert {
            0: 10,
            1: 10,
            2: 10,
            3: 10,
            4: 10,
            5: 10
        } == om.read_partition_offset_map(topic, partitions)


@pytest.mark.nocontainer
def test_commit_transaction(capsys, setup):
    (xcalarApi, xcalarClient, session) = setup
    with capsys.disabled():
        scope = get_or_create_session(xcalarClient, session.name).scope
        om = OffsetsManager(xcalarClient, scope)
        topic = f'test_kafka_{uuid.uuid1()}'
        kv_store = KvStore(xcalarClient, scope)
        partition_offset_map = {0: 10, 1: 10, 2: 10, 3: 10, 4: 10, 5: 10}
        om.commit_transaction(topic, partition_offset_map)
        for partition, offset in partition_offset_map.items():
            assert str(offset) == kv_store.lookup(
                om.get_new_partition_offset_key(topic, partition))

@pytest.mark.nocontainer
def test_kafka_app_receives_message_notification(capsys, setup_kafka):
    (kafka_configs, producers) = setup_kafka
    with capsys.disabled():
        publish_messages(kafka_configs, producers, 1)
        # Listen on all partitions, and expect a message back
        topic_info = TopicInfo(kafka_configs[0]['topic'], {0: 0, 1: 0, 2: 0})
        t = TopicHandler(
            topic_info, consumer_props=kafka_configs[0]['topic_props'])

        def checkResult(msg):
            assert msg is not None
            assert msg['key'] == b'test_key'

        t.handle(timeout_in_secs=TIMEOUT_IN_SECS, callback=checkResult)


def runApp(num_runs, configuration, topic, async_mode=True):
    print(f'Running kafka_{topic}_listener_app')
    xcalar_client = Client()
    if async_mode:
        groupId = runKafkaApp(
            f'{topic}_app', {
                'num_runs': num_runs,
                'configuration': configuration,
                'purge_interval_in_seconds': TIMEOUT_IN_SECS,
                'purge_cycle_in_seconds': TIMEOUT_IN_SECS
            },
            xcalar_client,
            async_mode=async_mode)
        print(f'Group ID: {groupId}')
        app = App(xcalar_client)
        while not app.is_app_alive(groupId):
            time.sleep(1)
        return groupId
    else:
        return runKafkaApp(
            f'{topic}_app', {
                'num_runs': num_runs,
                'configuration': configuration,
                'purge_interval_in_seconds': TIMEOUT_IN_SECS,
                'purge_cycle_in_seconds': TIMEOUT_IN_SECS
            },
            xcalar_client,
            async_mode=async_mode)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {}".format(err))
        raise Exception("Failed to deliver message: {}".format(err))

def publish_messages(kafka_configs, producers, num_runs):
    for kafka_config, producer in zip(kafka_configs, producers):
        for p in range(0, len(kafka_config['partition_offsets'])):
            num_messages = random.randrange(
                int(num_runs * kafka_config['batch_size'] * len(
                    kafka_config['partition_offsets'])))
            print(
                f'Publishing {num_messages} to topic:{kafka_config["topic"]}::partition:{p}'
            )
            for i in range(0, num_messages):
                producer.produce(kafka_config['topic'], 'test_message',
                                 'test_key', p,  on_delivery=acked)
    producer.flush()


@pytest.mark.nocontainer
def test_app_runs_end_to_end(capsys, setup, setup_kafka, configuration):
    (xcalarApi, xcalarClient, session) = setup
    (kafka_configs, producers) = setup_kafka
    num_runs = 2
    group_ids = []
    with capsys.disabled():
        try:
            publish_messages(kafka_configs, producers, num_runs)
            for config, kafka_config in zip(configuration, kafka_configs):
                group_ids.append(
                    runApp(
                        num_runs,
                        config,
                        kafka_config['topic'],
                        async_mode=False))
            table_name = f'{kafka_config["topic"]}'.upper()
            tables = session.list_tables(globally_shared_only=True)
            table_names = list(map(lambda t: t.name, tables))
            assert table_name in table_names
            for kafka_config in kafka_configs:
                table = session.get_table(table_name)
                assert table.record_count() > 0
                cross_session = get_or_create_session(xcalarClient,
                                                      f'cross_check_session')
                cross_table = Table(cross_session, table.fqn_name)
                assert cross_table.record_count() > 0
            table.unpublish()
        finally:
            app = App(xcalarClient)
            for group_id in group_ids:
                try:
                    app.cancel(group_id)
                except Exception:
                    pass

@pytest.mark.nocontainer
def test_purge(capsys, setup, setup_kafka, configuration):
    (xcalarApi, xcalarClient, session) = setup
    (kafka_configs, producers) = setup_kafka
    num_runs = 2
    with capsys.disabled():
        publish_messages(kafka_configs, producers, num_runs)
        (outStr, errStr) = runApp(num_runs, configuration[0],
                                  kafka_configs[0]['topic'], False)
        print(outStr)
        print(errStr)
        table_name = f'{kafka_configs[0]["topic"]}'.upper()
        tables = session.list_tables(
            pattern=table_name, globally_shared_only=True)
        assert len(tables) == 1
        original_record_count = tables[0].record_count()
        tables[0].show()
        print(f'Original record count: {original_record_count}')
        res_table_name = session.execute_sql(
            f'SELECT MAX(ts_nanos) fROM {table_name}')
        max_ts = next(
            session.get_table(res_table_name).records())['MAX_TS_NANOS']
        print(f'Max TS: {max_ts}')
        dataflow_invoker = DataflowInvoker(xcalarClient, session, xcalarApi, 0)
        dataflow_invoker.purge(kafka_configs[0]['topic'], tables[0].schema,
                               1 / 100)
        purged_record_count = session.get_table(table_name).record_count()
        print(f'Purged record count: {purged_record_count}')
        assert purged_record_count < original_record_count


@pytest.mark.nocontainer
def test_kafka_import_udf(capsys, setup_kafka, configuration, monkeypatch):
    (kafka_configs, producers) = setup_kafka
    import xcalar.container.context as ctx

    def get_xpu_id():
        return 0

    def get_node_id(id):
        return 0

    monkeypatch.setattr(ctx, 'get_xpu_id', get_xpu_id)
    monkeypatch.setattr(ctx, 'get_node_id', get_node_id)

    class MockCluster():
        def broadcast_msg(self, msg):
            pass

        def recv_msg(self):
            return json.dumps(kafka_configs[0])

        def barrier(self):
            time.sleep(1)

    mockCluster = MockCluster()
    mockCluster.xpus_per_node = [1]
    mockCluster._xpu_start_ids = [0]
    mockCluster.num_nodes = 1
    mockCluster.total_size = 1

    import xcalar.container.cluster as cluster

    def get_running_cluster():
        return mockCluster

    monkeypatch.setattr(cluster, 'get_running_cluster', get_running_cluster)

    with capsys.disabled():
        publish_messages(kafka_configs, producers, 1)
        kafka_config_fpath = f'/tmp/kafka_config_{uuid.uuid1()}.json'
        with open(kafka_config_fpath, 'w') as f:
            f.write(
                json.dumps({
                    'kafka_config': kafka_configs[0],
                    'partition_offset_map': {
                        '0': 0,
                        '1': 0,
                        '2': 0,
                        '3': 0,
                        '4': 0,
                        '5': 0,
                        '6': 0,
                        '7': 0,
                        '8': 0,
                        '9': 0
                    }
                }))

        partition_offset_map_fpath = tempfile.mktemp('.json',
                                                     'partition_offset_map')
        opts = {
            'kafka_app_node_id': 0,
            'kafka_app_xpu_id': 0,
            'kafka_props': kafka_config_fpath,
            'batch_size': 1,
            'session': 'test_kafka',
            'profile': False,
            'username': 'xdpadmin',
            'partition_offset_map_fpath': partition_offset_map_fpath
        }
        records = list(parse_kafka_topic(None, None, opts))
        assert len(records) > 0
        assert records[0]['key'] == "b'test_key'"
        assert records[0]['msg'] == "b'test_message'"
        with open(partition_offset_map_fpath) as f:
            partition_offsets = json.load(f)
            print(f'Got: {partition_offsets}')
            assert any(
                map(lambda x: x == 1,
                    partition_offsets['partition_offset_map'].values()))
