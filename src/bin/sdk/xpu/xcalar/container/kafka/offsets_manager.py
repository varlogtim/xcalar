from xcalar.external.kvstore import KvStore
from confluent_kafka import OFFSET_INVALID
import xcalar.container.context as ctx

logger = ctx.get_logger()


class OffsetsManager():
    def __init__(self, xcalar_client, scope):
        self.kv_store = KvStore(xcalar_client, scope)

    def get_consumed_partition_offset_key(self, topic, partition):
        return f'kafka_consumed_offsets::{topic}::{partition}'

    def get_new_partition_offsets_prefix(self, topic):
        return f'kafka_new_offsets::{topic}'

    def get_new_partition_offset_key(self, topic, partition):
        return f'{self.get_new_partition_offsets_prefix(topic)}::{partition}'

    def get_partition_offset_map(self, topic):
        logger.info(f'Getting offsets map for {topic}')
        partition_offset_map = {}
        for key in self.kv_store.list(
                f'{self.get_new_partition_offsets_prefix(topic)}::'):
            partition_offset_map[key] = self.kv_store.lookup(f'{key}')
        return partition_offset_map

    def read_partition_offset_map(self, topic, partitions):
        logger.info(f'Reading offsets map for {topic}::{partitions}')
        partition_offset_map = {}
        for partition in partitions:
            key = self.get_new_partition_offset_key(topic, partition)
            logger.info(f'Using key {key} to read offsets')
            if len(self.kv_store.list(key)) == 0:
                partition_offset_map[partition] = OFFSET_INVALID
            else:
                partition_offset_map[partition] = int(
                    self.kv_store.lookup(key))
        logger.info(
            f'New partition offset map for {topic}::{partitions} : {partition_offset_map}'
        )
        return partition_offset_map

    def read_consumed_partition_offset_map(self, topic, partitions):
        logger.info(f'Reading consumed offsets map for {topic}::{partitions}')
        partition_offset_map = {}
        for partition in partitions:
            key = self.get_consumed_partition_offset_key(topic, partition)
            if len(self.kv_store.list(key)) == 0:
                partition_offset_map[partition] = 0
            else:
                partition_offset_map[partition] = int(
                    self.kv_store.lookup(key))
        logger.info(
            f'Consumed partition offset map for {topic}::{partitions} : {partition_offset_map}'
        )
        return partition_offset_map

    def commit_offsets(self, topic, partition_offset_map):
        logger.info(f'Committing offsets for {topic}::{partition_offset_map}')
        for partition, offset in partition_offset_map.items():
            key = self.get_consumed_partition_offset_key(topic, partition)
            self.kv_store.add_or_replace(key, str(offset), True)
        logger.info(f'Committed partition offset map for {topic}')

    def commit_transaction(self, topic, partition_offset_map):
        logger.info(
            f'Consumed partition_offset_map for {topic} : {partition_offset_map}'
        )
        for partition, offset in partition_offset_map.items():
            key = self.get_new_partition_offset_key(topic, partition)
            logger.info(f'Using key {key} to commit offsets')
            self.kv_store.add_or_replace(key, str(offset), True)
        logger.info(f'Committed {topic} : {partition_offset_map}')
