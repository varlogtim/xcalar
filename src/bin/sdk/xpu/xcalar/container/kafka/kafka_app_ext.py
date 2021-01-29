import argparse
from datetime import datetime
from xcalar.container.kafka.kafka_app_runner import runKafkaAppFromShell, stopKafkaApp, is_app_alive
from tabulate import tabulate
from xcalar.container.kafka.offsets_manager import OffsetsManager
from xcalar.solutions.tools.connector import XShell_Connector

from xcalar.container.kafka.kafka_broker_test import test_brocker

import json

def dispatcher(line):
    if not XShell_Connector.self_check_client():
        return None

    tokens = line.split(' ')
    parser = argparse.ArgumentParser(
        prog='kafka', description='Argument parser for Kafka Streaming App')
    sub_parsers = parser.add_subparsers(help='start/stop a kafka app')

    stop_parser = sub_parsers.add_parser('stop', help='Stop a kafka app')
    stop_parser.add_argument(
        '--kafka_topic',
        required=True,
        help='Stop kafka app listening on this topic')
    stop_parser.set_defaults(func=stop)

    test_parser = sub_parsers.add_parser('test', help='Test kafka conectivity')
    test_parser.add_argument(
        '--kafka_broker',
        required=True,
        help='Kafka broker URL')
    test_parser.add_argument(
        '--kafka_topic',
        required=False,
        default='TestTopic',
        help='Test topic name')
    test_parser.set_defaults(func=test_brocker_ping)

    stream_parser = sub_parsers.add_parser('stream', help='Start a kafka app')
    # stream_parser.add_argument(
    #     '--kafka_topic',
    #     required=True,
    #     help='Stream kafka app listening on this topic')
    stream_parser.add_argument(
        '--num_runs', required=False, default=1, help='Number of runs')
    stream_parser.add_argument(
        '--kafka_properties',
        required=True,
        help='Path to Kafka properties file (on the cluster)')
    stream_parser.add_argument(
        '--profile',
        required=False,
        action='store_true',
        help='Produce profile information')
    stream_parser.add_argument(
        '--async_mode',
        required=False,
        action='store_true',
        help='Run in async mode')
    stream_parser.add_argument(
        '--purge_interval_in_seconds',
        required=False,
        default=None,
        help='Purge messages older than X seconds')
    stream_parser.add_argument(
        '--purge_cycle_in_seconds',
        required=False,
        default=None,
        help='Run purge every X seconds')
    stream_parser.add_argument(
        '--alias',
        required=False,
        default=None,
        help='Use alias instead of topic name for data table name')
    stream_parser.set_defaults(func=stream)

    status_parser = sub_parsers.add_parser(
        'status', help='Check status of a kafka app')
    status_parser.add_argument(
        '--kafka_topic', required=True, help='Kafka topic')
    status_parser.set_defaults(func=check_status)

    offsets_parser = sub_parsers.add_parser(
        'offsets', help='Manage kafka offsets')
    offsets_sub_parsers = offsets_parser.add_subparsers(
        help='Various offsets actions')

    offsets_view_parser = offsets_sub_parsers.add_parser(
        'view', help='View kafka offsets')
    offsets_view_parser.add_argument(
        '--kafka_topic', required=True, help='Kafka topic')
    offsets_view_parser.set_defaults(func=view_offsets)

    offsets_adjust_parser = offsets_sub_parsers.add_parser(
        'adjust', help='Adjust kafka offsets')
    offsets_adjust_parser.add_argument(
        '--kafka_topic', required=True, help='Kafka topic')
    offsets_adjust_parser.add_argument(
        '--partition_offset',
        required=True,
        help='Partition, Offset for example: 0:200',
        nargs='+')
    offsets_adjust_parser.set_defaults(func=adjust_offsets)

    args = parser.parse_args(tokens)
    args.xcalar_client = XShell_Connector.get_client()
    args.xcalar_session = XShell_Connector.get_session_obj()
    args.func(args)

def test_brocker_ping(args):
    test_brocker(args.kafka_topic, args.kafka_broker)


def check_status(args):
    if is_app_alive(args.xcalar_client, args.kafka_topic):
        print('Running')
    else:
        print('Not running')

def adjust_offsets(args):
    offsets_mgr = OffsetsManager(args.xcalar_client, args.xcalar_session.scope)
    partition_offset_map = dict(
        map(lambda x: x.split(':'), args.partition_offset))
    offsets_mgr.commit_transaction(args.kafka_topic, partition_offset_map)
    print(f'Committed new offsets.')

def get_offsets(session, topic):
    offsets_mgr = OffsetsManager(session.client, session.scope)
    partition_offset_values = [
        (k.split('::')[2], v)
        for k, v in offsets_mgr.get_partition_offset_map(topic).items()
    ]
    table = sorted(partition_offset_values, key=lambda x: x[0])
    return table

def view_offsets(args):
    table = get_offsets(args.xcalar_session, args.kafka_topic)
    print(tabulate(table, ['Partition', 'Offset'], tablefmt='psql'))

def stop(args):
    stopKafkaApp(args.xcalar_client, args.kafka_topic)

def stream(args):
    configuration = {'session_name': args.xcalar_session.name}
    with open(args.kafka_properties) as cp:
        configuration['kafka_config'] = json.load(cp)    
    
    kafka_topc_app = args.alias if args.alias else configuration['kafka_config']['topic']
    if is_app_alive(args.xcalar_client, kafka_topc_app):
        print(f'{kafka_topc_app} is already running')
        return

    start_dt = datetime.utcnow()
    app_name = f'kafka_app_runner_{datetime.utcnow().strftime("%s")}'
    app = runKafkaAppFromShell(
        app_name,
        configuration,
        args.xcalar_session.name,
        args.xcalar_client.username,
        args.xcalar_client,
        args.num_runs,
        args.profile,
        async_mode=args.async_mode,
        purge_interval_in_seconds=args.purge_interval_in_seconds,
        purge_cycle_in_seconds=args.purge_cycle_in_seconds,
        alias=args.alias)
    print(f'App name: {kafka_topc_app}')
    print(f'Elapsed time: {(datetime.utcnow() - start_dt).seconds} seconds')

def load_ipython_extension(ipython, *args):
    # use ipython.register_magic_function
    #   to register "dispatcher" function
    #   make magic_name = 'http'
    ipython.register_magic_function(dispatcher, 'line', magic_name='kafka')

def unload_ipython_extension(ipython):
    pass
