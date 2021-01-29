import inspect
import json
import logging
import os
import sys
import traceback
import uuid
import tempfile
from contextlib import contextmanager
import cProfile
import pstats
import io
import xcalar.container.context as ctx
from xcalar.container.cluster import get_running_cluster
from xcalar.container.kafka.custom_logger import CustomLogger
from xcalar.container.kafka.dataflow_invoker import DataflowInvoker
from xcalar.container.kafka.kafka_metadata import KafkaMetadata
from xcalar.container.kafka.offsets_manager import OffsetsManager
from xcalar.container.kafka.topic_handler import TopicHandler, TopicInfo
from xcalar.container.kafka.xcalar_client_utils import (get_or_create_session)
from xcalar.container.kafka.kafka_app_runner import get_pidfile_path
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.container.kafka.lib_loader import dynaload
from datetime import datetime
import math

clogger = CustomLogger(ctx.get_logger())


@contextmanager
def pidfile(path):
    try:
        with open(path, 'w') as fp:
            yield fp
    finally:
        try:
            os.remove(path)
        except IOError:
            sys.stderr.write(f'Failed to delete {path}')


class KafkaApp():
    def __init__(self,
                 configuration,
                 profile,
                 username,
                 purge_interval_in_seconds=300,
                 purge_cycle_in_seconds=30,
                 alias=None):
        self.configuration = configuration
        self.kafka_config = self.configuration['kafka_config']
        self.topic = self.kafka_config['topic']
        clogger.log(logging.INFO, f'{self.configuration}')
        module = dynaload(self.kafka_config['parser_udf_name'],
                          self.kafka_config['parser_udf_path'])
        self.schema = self.add_system_fields(module.get_schema(self.topic))
        self.profile = profile
        self.username = username
        self.total_bytes_consumed = 0
        self.purge_interval_in_seconds = purge_interval_in_seconds
        self.purge_cycle_in_seconds = purge_cycle_in_seconds
        self.last_purge_time = datetime.utcnow()
        self.alias = alias
        clogger.prefix = f'{self.topic if self.alias is None else self.alias}'

    def add_system_fields(self, schema):
        schema['ts_nanos'] = 'float'
        schema['topic'] = 'string'
        schema['index'] = 'string'
        schema['xpu_id'] = 'int'
        return schema

    def get_xpu_and_node_id(self):
        cluster = get_running_cluster()
        my_node_id = cluster.my_node_id
        my_xpu_id = cluster.my_xpu_id
        return (my_node_id, my_xpu_id)

    def create_or_update_udf(self):
        client = Client(bypass_proxy=True, user_name=self.username)
        session = get_or_create_session(client,
                                        self.configuration['session_name'])
        workbook = client.get_workbook(session.name)
        clogger.log(
            logging.INFO,
            f'Creating udf on client: {client._url}, session:{session.name}, workbook: {workbook.name}'
        )
        udf_name = f'kafka_ingest_{self.topic}_udf'
        import xcalar.container.kafka.kafka_import_udf as us
        udf_source = inspect.getsource(us)
        if len(workbook.list_udf_modules(udf_name)) > 0:
            # logger.info(f'deleting {udf_name}')
            # udf = xcalarClient.get_udf_module(udf_name)
            # udf.update(udf_source)
            # logger.info(f'deleting {udf_name}')
            # return udf_name
            # Not recreating the UDF here
            clogger.log(
                logging.INFO,
                f'found an existing version of {udf_name}, not recreating')
            return udf_name
        else:
            clogger.log(logging.INFO, f'creating {udf_name}')
            workbook.create_udf_module(udf_name, udf_source)
            clogger.log(logging.INFO, f'created udf {udf_name}')
            return udf_name

    def execute_udf(self, udf_name, kafka_config_fpath):
        try:
            xcalarClient = Client(bypass_proxy=True, user_name=self.username)
            xcalarApi = XcalarApi(bypass_proxy=True)
            session = get_or_create_session(xcalarClient,
                                            self.configuration['session_name'])
            xcalarApi.setSession(session)
            (my_node_id, my_xpu_id) = self.get_xpu_and_node_id()
            df = DataflowInvoker(xcalarClient, session, xcalarApi, my_xpu_id)
            if self.is_time_to_purge(session, self.topic):
                self.last_purge_time = datetime.utcnow()
                df.purge(
                    self.topic,
                    self.schema,
                    self.purge_interval_in_seconds,
                    alias=self.alias)
            return df.execute_udf(
                self.topic,
                udf_name,
                self.schema, {
                    'kafka_app_node_id':
                        my_node_id,
                    'kafka_app_xpu_id':
                        my_xpu_id,
                    'kafka_props':
                        kafka_config_fpath,
                    'batch_size':
                        self.kafka_config['batch_size'],
                    'session':
                        self.configuration['session_name'],
                    'profile':
                        self.profile,
                    'username':
                        self.username,
                    'partition_offset_map_fpath':
                        self.partition_offset_map_fpath
                },
                alias=self.alias)
        finally:
            try:
                os.remove(kafka_config_fpath)
            except Exception:
                pass

    def is_time_to_purge(self, session, topic):
        return len(session.list_tables(topic.upper())) == 1 and (
            (datetime.utcnow() - self.last_purge_time).seconds >
            self.purge_cycle_in_seconds)

    def load_messages(self, notification_msg):
        kafka_config_fpath = f'{tempfile.gettempdir()}/kafka_config_{uuid.uuid1()}.json'
        with open(kafka_config_fpath, 'w') as f:
            json.dump({
                'kafka_config': self.kafka_config,
                'partition_offset_map': self.partition_offsets
            }, f)
            f.flush()
            os.fsync(f.fileno())
        udf_name = None
        try:
            udf_name = self.create_or_update_udf()
            return self.execute_udf(udf_name, kafka_config_fpath)
        except Exception:
            clogger.log(logging.ERROR,
                        f'Failed to run {udf_name}\n{traceback.format_exc()}')
            raise Exception(f'Failed to run {udf_name}')

    def run(self, num_runs):
        cluster = get_running_cluster()
        (my_node_id, my_xpu_id) = self.get_xpu_and_node_id()
        master_node_id = ctx.get_node_id(cluster.master_xpu_id)
        is_local_master = True
        if my_node_id != master_node_id:
            is_local_master = cluster.is_local_master()
        else:
            is_local_master = cluster.is_master()
        clogger.log(
            logging.INFO,
            f'my_node_id: {my_node_id}, master_node_id: {master_node_id}, is_local_master: {is_local_master}'
        )
        if self.alias is not None:
            path = get_pidfile_path(self.alias)
        else:
            path = get_pidfile_path(self.topic)
        clogger.log(logging.INFO, f'pidfile_path: {path}')
        # Static one-time work
        if cluster.is_master() and my_node_id == master_node_id:
            clogger.log(logging.INFO,
                        f'Checking if kafka-base data target exists.')
            xcalarClient = Client(bypass_proxy=True, user_name=self.username)
            targets = list(
                filter(
                    lambda name: name == 'kafka-base',
                    map(lambda dt: dt.name, xcalarClient.list_data_targets())))
            if len(targets) == 0:
                # data_target = xcalarClient.get_data_target(target_name="kafka-base")
                # data_target.delete()
                xcalarClient.add_data_target(
                    target_name="kafka-base", target_type_id="memory")

        if is_local_master:
            if os.path.exists(path):
                raise Exception(
                    f'kafka_{self.topic}_listener_app exists. Exiting.')
            else:
                clogger.log(logging.INFO, f'Running listener now.')
                with pidfile(path) as fp:
                    fp.write('{}'.format(os.getpid()))
                    fp.flush()
                    run = 0
                    xcalar_client = Client(
                        bypass_proxy=True, user_name=self.username)
                    topic = self.kafka_config['topic']
                    consumer_props = self.kafka_config['topic_props']
                    metadata = KafkaMetadata(consumer_props=consumer_props)
                    offsets_manager = OffsetsManager(
                        xcalar_client,
                        get_or_create_session(
                            xcalar_client,
                            self.configuration['session_name']).scope)
                    partition_props = self.kafka_config['partition_offsets']
                    offsets_key = self.alias if self.alias is not None else topic
                    if len(
                            offsets_manager.get_partition_offset_map(
                                offsets_key)) == 0:
                        clogger.log(
                            logging.INFO,
                            f'No offsets found for {offsets_key}, starting with initial offsets'
                        )
                        offsets_manager.commit_transaction(
                            offsets_key, partition_props)
                    self.partition_offsets = None
                    while run < float(num_runs):
                        clogger.log(logging.INFO, f'Starting run#{run}')
                        self.partition_offset_map_fpath = tempfile.mktemp(
                            '.json', 'partition_offset_map')
                        try:
                            if self.partition_offsets is None:
                                self.partition_offsets = offsets_manager.read_partition_offset_map(
                                    offsets_key,
                                    metadata.get_partitions(topic))
                            clogger.log(
                                logging.INFO,
                                f'partition_offsets: {self.partition_offsets}')
                            topic_info = TopicInfo(topic,
                                                   self.partition_offsets)
                            t = TopicHandler(
                                topic_info, consumer_props=consumer_props)
                            t.handle(
                                timeout_in_secs=self.
                                kafka_config['poll_timeout_in_secs'],
                                callback=self.load_messages)
                            clogger.log(
                                logging.INFO,
                                f'Consumed messages successfully, now committing offsets from {self.partition_offset_map_fpath}'
                            )
                            response = {}
                            with open(self.partition_offset_map_fpath) as f:
                                response = json.load(f)
                                self.partition_offsets = response[
                                    'partition_offset_map']
                                offsets_manager.commit_transaction(
                                    offsets_key, self.partition_offsets)
                            self.total_bytes_consumed = self.total_bytes_consumed + response[
                                "total_bytes_consumed"]
                            clogger.log(
                                logging.INFO,
                                f'Finished run#{run}, consumed {response["total_bytes_consumed"]} bytes, total_bytes_consumed: {self.total_bytes_consumed}'
                            )
                            run = run + 1
                        finally:
                            try:
                                os.remove(self.partition_offset_map_fpath)
                            except Exception:
                                pass


def main(inBlob):
    clogger.log(
        logging.INFO,
        f'kafka_app called with args [ {json.dumps(inBlob, indent=4)} ]')
    args = json.loads(inBlob)
    if 'profile' in args and args['profile']:
        pr = cProfile.Profile()
        pr.enable()
    app = KafkaApp(
        args['configuration'],
        profile='profile' in args and args['profile'],
        username='username' in args and args['username'],
        purge_interval_in_seconds=int(args['purge_interval_in_seconds']),
        purge_cycle_in_seconds=int(args['purge_cycle_in_seconds']),
        alias=args['alias'] if 'alias' in args else None)
    if args['async_mode']:
        clogger.log(logging.INFO, f'kafka_app running in async mode')
        app.run(math.inf)
    else:
        clogger.log(logging.INFO, f'kafka_app running in sync mode')
        app.run(args['num_runs'])

    if 'profile' in args and args['profile']:
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        clogger.log(logging.INFO, s.getvalue())
