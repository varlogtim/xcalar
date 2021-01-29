import json
import logging
import traceback
from datetime import datetime
import os
from confluent_kafka import Consumer, TopicPartition
import cProfile
import pstats
import io
import xcalar.container.cluster as cl
import xcalar.container.context as ctx
from xcalar.container.kafka.custom_logger import CustomLogger
from xcalar.container.kafka.kafka_metadata import KafkaMetadata
from xcalar.container.kafka.partition_manager import PartitionManager
from xcalar.container.kafka.lib_loader import dynaload
import time

clogger = CustomLogger(ctx.get_logger())


def read_kafka_props(kafka_props_fpath, kafka_app_xpu_id):
    kafka_props = None
    cluster = cl.get_running_cluster()
    current_xpu_id = ctx.get_xpu_id()
    if current_xpu_id == kafka_app_xpu_id:
        clogger.log(logging.INFO, f'Loading {kafka_props_fpath} now')
        with open(kafka_props_fpath) as f:
            kafka_props = json.load(f)
        clogger.log(logging.INFO,
                    f'Loaded {kafka_props}, now broadcasting it!')
        cluster.broadcast_msg(json.dumps(kafka_props))
        clogger.log(logging.INFO,
                    f'Successfully broadcasted {json.dumps(kafka_props)}!')
    else:
        clogger.log(logging.INFO,
                    f'Waiting for {kafka_props_fpath} to be broadcasted')
        kafka_props = json.loads(cluster.recv_msg())
        clogger.log(logging.INFO, f'Successfully received {kafka_props}!')
    return kafka_props


def create_topic_partition_list(topic, partition_offset_map):
    topic_partition_list = []
    for partition, offset in partition_offset_map.items():
        clogger.log(logging.INFO,
                    f'Assigning partition:{partition} :: offset:{offset}')
        topic_partition_list.append(
            TopicPartition(topic, int(partition), int(offset)))
    return topic_partition_list


def log_kv_value(key, kv_store, scope):
    kvinfo = kv_store.keyList(f'{key}.*', scope)
    clogger.log(logging.INFO, f'KVSTORE: {kvinfo}')


def size_accumulator(kafka_stream, collector):
    for msg in kafka_stream:
        collector['size'] = collector['size'] + len(msg)
        yield msg


def kafka_iter(consumer, batch_size, batch_timeout_in_secs,
               partition_offset_map):
    start_dt = datetime.utcnow()
    total_elapsed_time_in_secs = batch_timeout_in_secs
    clogger.log(
        logging.INFO,
        f'total_elapsed_time_in_secs allowed {total_elapsed_time_in_secs}')
    messages_consumed = 0
    while messages_consumed < batch_size and (
            datetime.utcnow() - start_dt).seconds < total_elapsed_time_in_secs:
        msg = consumer.poll(batch_timeout_in_secs)
        if msg is None or msg.error():
            error = None if msg is None else msg.error().code
            clogger.log(logging.INFO,
                        f'Failed to read next message error {error}')
            break
        messages_consumed = messages_consumed + 1
        yield msg
        # increment offset
        if msg.partition() in partition_offset_map:
            partition_offset_map[
                msg.partition()] = partition_offset_map[msg.partition()] + 1
        else:
            partition_offset_map[msg.partition()] = 1


def parse_kafka_topic(fullPath, inStream, opts):
    pr = None
    if 'profile' in opts:
        pr = cProfile.Profile()
        pr.enable()

    clogger.prefix = f'node_id:{ctx.get_node_id(ctx.get_xpu_id())}::xpu_id:{ctx.get_xpu_id()}'
    clogger.log(logging.INFO,
                f'kafka_import_udf:parse_kafka_topic invoked with {opts}')
    cluster = cl.get_running_cluster()
    clogger.log(
        logging.INFO,
        f'xpus_per_node:{cluster.xpus_per_node}, total_size:{cluster.total_size}, num_nodes:{cluster.num_nodes}, xpu_start_ids: {cluster._xpu_start_ids}'
    )

    kafka_app_xpu_id = opts['kafka_app_xpu_id']
    kafka_props_fpath = opts['kafka_props']
    try:
        current_xpu_id = ctx.get_xpu_id()
        kafka_config = read_kafka_props(kafka_props_fpath, kafka_app_xpu_id)
        kafka_props = kafka_config['kafka_config']
        topic = kafka_props['topic']
        consumer_props = {'debug': 'consumer'}
        consumer_props.update(kafka_props['topic_props'])
        consumer = Consumer(consumer_props)
        metadata = KafkaMetadata(consumer=consumer)
        partitions = metadata.get_partitions(topic)
        partition_manager = PartitionManager(clogger)
        xpu_partition_map = partition_manager.xpu_partition_map(
            cluster.num_nodes, cluster._xpu_start_ids, cluster.xpus_per_node,
            partitions)

        if current_xpu_id not in xpu_partition_map:    # NOQA
            clogger.log(
                logging.INFO,
                f'No partition for me. xpu_partition_map: {xpu_partition_map}')
            # cluster.send_msg(kafka_app_xpu_id, {})
            return
        clogger.log(
            logging.INFO,
            f'I am assigned a partition! xpu_partition_map: {xpu_partition_map}'
        )
        partition_offset_map = {}
        all_partition_offset_map = kafka_config['partition_offset_map']
        clogger.log(
            logging.INFO,
            f'Received partition_offset_map {all_partition_offset_map}')
        for partition in xpu_partition_map[current_xpu_id]:
            partition_offset_map[partition] = all_partition_offset_map[str(
                partition)]
        clogger.log(logging.INFO,
                    f'partition_offset_map: {partition_offset_map}')
        topic_partition_list = create_topic_partition_list(
            topic, partition_offset_map)
        clogger.log(logging.INFO,
                    f'topic_partition_list: {topic_partition_list}')
        consumer.assign(topic_partition_list)
        batch_timeout_in_secs = kafka_props['batch_timeout_in_secs']
        clogger.log(
            logging.INFO,
            f'Start consuming from topic:{topic} :: partition:{topic_partition_list}'
        )
        batch_size = opts['batch_size']
        clogger.log(logging.INFO, f'Using batch_size: {batch_size}')

        messages_consumed = 0
        module = dynaload(kafka_props['parser_udf_name'],
                          kafka_props['parser_udf_path'])
        collector = {'size': 0}
        context = {'xpu_id': current_xpu_id}
        batch_ts_nanos = time.time() * 1000 * 1000 * 1000
        for record in module.parse_kafka_stream(
                size_accumulator(
                    kafka_iter(consumer, batch_size, batch_timeout_in_secs,
                               partition_offset_map), collector), context):
            record['ts_nanos'] = time.time() * 1000 * 1000 * 1000
            record['topic'] = topic
            record['xpu_id'] = int(current_xpu_id)
            record['index'] = str(f'{int(batch_ts_nanos)}{current_xpu_id:03}')
            yield record
            messages_consumed = messages_consumed + 1

        clogger.log(
            logging.INFO,
            f'Committing offsets topic:{topic} :: partition_offset_map:{partition_offset_map}'
        )
        if ctx.get_xpu_id() != kafka_app_xpu_id:
            clogger.log(logging.INFO, f'Sending offsets to {kafka_app_xpu_id}')
            cluster.send_msg(
                kafka_app_xpu_id, {
                    'xpu_id': ctx.get_xpu_id(),
                    'partition_offset_map': partition_offset_map,
                    'size': collector['size']
                })
        else:
            max_messages = len(xpu_partition_map.keys()) - 1
            clogger.log(logging.INFO,
                        f'Waiting for {max_messages} messages to arrive')
            total_bytes_consumed = 0
            for index in range(0, max_messages):
                clogger.log(logging.INFO, f'Waiting for message {index}')
                msg = cluster.recv_msg()
                clogger.log(logging.INFO, f'Received {msg}')
                partition_offset_map.update(msg['partition_offset_map'])
                total_bytes_consumed = total_bytes_consumed + msg['size']
            clogger.log(
                logging.INFO,
                f'Writing {partition_offset_map} to output file {opts["partition_offset_map_fpath"]}'
            )
            with open(opts['partition_offset_map_fpath'], 'w') as f:
                json.dump({
                    'partition_offset_map': partition_offset_map,
                    'total_bytes_consumed': total_bytes_consumed
                }, f)
                f.flush()
                os.fsync(f.fileno())
        clogger.log(
            logging.INFO,
            f'Consumed {messages_consumed} messages from topic:{topic} :: partition:{topic_partition_list}'
        )

    except Exception as e:
        clogger.log(logging.ERROR,
                    f'Failed to consume messages {traceback.format_exc()}')
        raise e

    if 'profile' in opts and opts['profile']:
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        clogger.log(logging.INFO, s.getvalue())
