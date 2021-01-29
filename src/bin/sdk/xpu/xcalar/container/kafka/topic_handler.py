import multiprocessing as mp
import queue

from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException

import xcalar.container.context as ctx

logger = ctx.get_logger()
"""
A structure to store topic and partition offset map

Example:
  topic = 'test'
  partition_offset_map = {0:100, 1:200}
"""


class TopicInfo():
    def __init__(self, topic, partition_offset_map):
        self.topic = topic
        self.partition_offset_map = partition_offset_map


"""
A topic handler that accepts topic info and consumer properties.
It contains a handle() method that listens to multiple partitions in a topic and calls a
user-provided callback on receiving the first message.

Raises:
    Exception: if no message was received on any partition within the given timeout

Returns:
    None
"""


class TopicHandler():
    def __init__(self, topic_info, consumer_props=None):
        self.topic_info = topic_info
        self.consumer_props = consumer_props

    @staticmethod
    def receiveMessage(q, consumer_props, topic, partition, offset,
                       timeout_in_secs):
        llogger = ctx.get_logger()
        llogger.info(f'Listening for new messages on {topic}::{partition}')
        c = Consumer(consumer_props)
        try:
            c.assign([TopicPartition(topic, int(partition), int(offset))])
            msg = c.poll()
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    llogger.info(
                        f'Unknown error [{msg.error()}] during polling.')
                    raise KafkaException(msg.error())
            elif msg is not None:
                llogger.info(
                    f'Topic: {topic}:: Partition: {partition} - Got message at offset {msg.offset()}'
                )
                q.put({
                    'headers': msg.headers(),
                    'error': msg.error(),
                    'key': msg.key(),
                    'topic': msg.topic(),
                    'offset': msg.offset(),
                    'partition': msg.partition(),
                    'ts': msg.timestamp()
                })
        finally:
            c.close()

    def killProcesses(self, processes):
        for p in processes:
            logger.info(f'Killing process with pid:{p.pid}')
            try:
                if p.is_alive():
                    p.terminate()
            except Exception as e:
                logger.warn(f'Failed to kill process {p.pid}, error: {str(e)}')

    def listen(self, timeout_in_secs):
        processes = []
        try:
            q = mp.Queue()
            for partition, offset in self.topic_info.partition_offset_map.items(
            ):
                p = mp.Process(
                    target=TopicHandler.receiveMessage,
                    args=(q, self.consumer_props, self.topic_info.topic,
                          partition, offset, timeout_in_secs))
                p.start()
                processes.append(p)
            msg = q.get(timeout=timeout_in_secs)
            logger.info(f'Received new message:\n{msg}')
            return msg
        except queue.Empty:
            errMsg = f'Failed to receive message within {timeout_in_secs} seconds'
            logger.error(errMsg)
            raise Exception(errMsg)
        finally:
            self.killProcesses(processes)

    def handle(self, timeout_in_secs=30, callback=None):
        # Assumption: the number of partitions in a topic do not change
        msg = self.listen(timeout_in_secs)
        if callback is not None:
            return callback(msg)
