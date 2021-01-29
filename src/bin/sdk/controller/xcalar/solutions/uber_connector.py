from structlog import get_logger
import json

logger = get_logger(__name__)


def sampleXpuPartitionMap(batch_id, offsets):
    """
    Deafault/sample implementation of xpu-topi-partition map algorithm.
    Should be supplied for each implementation
    """
    kafka_props = {
        'enable_auto_commit': False,
        'bootstrap_servers': ['xcalar-deps305:9092'],
        'group_id': 'xcalar-test-group',
        'consumer_timeout_ms': 3000
    }
    parserArg = {
        'maxIter': 15,
        'batch_id': batch_id,
        'metadataInfo': {
            '0': {
                'kafka_props': kafka_props,
                'topic': 'narrow_1',
                'partitionInfo': {
                    '1': {
                        'min': 0,
                        'max': 998999
                    }
                }
            }
        }
    }
    for record in offsets:
        if record['topic'] == '-':
            continue
        try:
            next_offset = int(record['max_offset']) % 9999000 + 1
            parserArg = {
                'maxIter': 100,
                'batch_id': batch_id,    # 10000
                'metadataInfo': {
                    str(i): {
                        'kafka_props': kafka_props,
                        'topic': record['topic'],
                        'partitionInfo': {
                            str(record['partition']): {
                                'min': next_offset,
                                'max': 9999999
                            }
                        }
                    }
                    for i in range(0, 20)
                }
            }
        except Exception as e:
            logger.debug('{} Error reading kafka offsets, {}'.format(
                e, record))
    return parserArg


class Connector:
    """
    Kafka connector Class.
    Pulls data from kafka given specified partition map, latest ofssets and batch id.
    """

    def __init__(
            self,
    #  import_udf_template,
            import_udf_name,
            xpuPartitionMap=sampleXpuPartitionMap):
        # self.kafka_import_udf_template = import_udf_template
        self.import_udf_name = import_udf_name
        self.xpuPartitionMap = xpuPartitionMap
        self.universe = {}

    def nextBatch(self, batch_id, offsets):
        """
        Computes xpu-topic-partiton map giving provided function
        Passes computed map to the UDF.
        Pulls next batch from kafka into an Uber delta staging table.
        """
        xpu_partition_map = self.xpuPartitionMap(batch_id, offsets)
        kafka_universe = {
            k: self.universe[k]
            for k in self.universe
            if 'kafka' == self.universe[k].get('stream', {
                'type': None
            }).get('type', 'kafka')
        }
        params = {
            "opts": xpu_partition_map,
            "universe": {
                kafka_universe[k]['stream'].get('path', k): k
                for k in kafka_universe
            }
        }
        key = self.dispatcher.send_2kvstore(json.dumps(params))
        logger.debug(f'Params: {params}')
        logger.debug('Use these args to test UDF in XD: ' + '{' +
                     f'"key": {key}' + '}')
        dsetName = None
        for try_counts in range(1, 6):
            try:
                uber_table, dsetName = \
                    self.dispatcher.loadStreamNonOptimized(data_target=self.targetName,
                                                           parser_args={"key": key},
                                                           parser_name='{}:parse_kafka_topic'.format(self.import_udf_name))
                return (uber_table, dsetName)
            except Exception as e:
                logger.debug(
                    '***: Warning processing dataset: {}, attempt# {}'.format(
                        str(e), try_counts))
                try:
                    if dsetName is not None:
                        logger.debug(f'deleting dataset ... {dsetName}')
                        dataset = self.dispatcher.xcalarClient.get_dataset(
                            dsetName)
                        dataset.delete(delete_completely=True)
                        self.dispatcher.datasetCounter -= 1
                except Exception:
                    logger.warning(
                        f'***: Warning: unable to delete dataset {dsetName}')
        raise Exception(
            '***: 5 unseccessful attempts to create Uber dataset, exiting')
