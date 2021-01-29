import unittest
from unittest.mock import patch
import xcalar.solutions.udf.kafka_import_confluent
from xcalar.solutions.udf.kafka_import_confluent import parse_kafka_topic
import logging
from kafka_utils import SingletonKafkaUtils
import uuid

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] p%(process)s {%(filename)s:%(lineno)d} '
    '%(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()])


class TestImportUdf(unittest.TestCase):
    def setUp(self):
        MAX_MESSAGE_COUNT = 10
        self.kafka_utils = SingletonKafkaUtils.getInstance()
        self.kafka_utils.publishToKafka(MAX_MESSAGE_COUNT)
        self.expected_ds1 = {
            'batch_id': 2,
            'xpu_id': '0',
            'topic': self.kafka_utils.INPUT_TOPIC_1,
            'timestamp': 1570673173125.609,
            'partition': 0,
            'dataset': 'ds1',
            'offset': 0,
            'record': {
                'col8': 8829392.0,
                'col12': 44002800000000.0,
                'col9': 'Duobam',
                'col11': 442305583714000.0,
                'col6': 2020,
                'col10': '0.06',
                'col7': '6th generation',
                'col4': 'Doug Kuhic',
                'col5': -18799.9564,
                'col2': '4650 Ryan Groves, East Millard, NV 84870',
                'col3': 'Asteria',
                'col1': 1
            }
        }

        self.expected_ds2 = {
            'batch_id': 2,
            'xpu_id': '0',
            'topic': self.kafka_utils.INPUT_TOPIC_1,
            'timestamp': 1570672134405.442,
            'partition': 0,
            'dataset': 'ds2',
            'offset': 99,
            'record': {
                'd4': 'Luthor Tyrell',
                'd5': -75526.22072,
                'd6': 61087,
                'd1': 1,
                'd2': 'Westhighland Terrier',
                'd3': 'Rembrandt'
            }
        }

        self.kafka_opts = {
            'maxIter': MAX_MESSAGE_COUNT,
            'batch_id': 2,
            'metadataInfo': {
                '0': {
                    'kafka_props': {
                        'bootstrap.servers': self.kafka_utils.KAFKA_BROKER,
                        'group.id': f'CG1_{uuid.uuid1()}',
                        'enable.auto.commit': 'false',
                        'default.topic.config': {
                            'auto.offset.reset': 'smallest'
                        }
                    },
                    'topic': self.kafka_utils.INPUT_TOPIC_1,
                    'partitionInfo': {
                        '0': {
                            'min': 0,
                            'max': 9999999
                        }
                    }
                },
                '1': {
                    'kafka_props': {
                        'bootstrap.servers': self.kafka_utils.KAFKA_BROKER,
                        'group.id': f'CG1_{uuid.uuid1()}',
                        'enable.auto.commit': 'false',
                        'default.topic.config': {
                            'auto.offset.reset': 'smallest'
                        }
                    },
                    'topic': self.kafka_utils.INPUT_TOPIC_2,
                    'partitionInfo': {
                        '0': {
                            'min': 0,
                            'max': 9999999
                        }
                    }
                },
                '2': {
                    'kafka_props': {
                        'bootstrap.servers': self.kafka_utils.KAFKA_BROKER,
                        'group.id': f'CG1_{uuid.uuid1()}',
                        'enable.auto.commit': 'false',
                        'default.topic.config': {
                            'auto.offset.reset': 'smallest'
                        }
                    },
                    'topic': self.kafka_utils.INPUT_TOPIC_3,
                    'partitionInfo': {
                        '0': {
                            'min': 0,
                            'max': 9999999
                        }
                    }
                }
            }
        }

    def tearDown(self):
        self.kafka_utils.cleanup()

    @patch('xcalar.solutions.udf.kafka_import_confluent.ctx')
    def test_valid_messages(self, mock_ctx):
        mock_ctx.get_xpu_id.return_value = 0
        xcalar.solutions.udf.kafka_import_confluent.opts = self.kafka_opts
        xcalar.solutions.udf.kafka_import_confluent.universe = {
            'ds1': 'ds1',
            'ds2': 'ds2'
        }
        records = parse_kafka_topic(None, None)
        for r in records:
            try:
                assert r['dataset'] != 'info', 'Found an error message'
                assert 'timestamp' in r.keys()
                assert r['dataset'] in ['ds1', 'ds2']
                assert r['offset'] in range(0, 100)
                if r['dataset'] == 'ds1':
                    expected = self.expected_ds1
                else:
                    expected = self.expected_ds2
                keys = ['batch_id', 'xpu_id', 'topic', 'partition']
                for k in keys:
                    assert expected[k] == r[
                        k], 'Failed to compare key: {}, expected: {}, actual: {}'.format(
                            k, expected[k], r[k])

                for k in expected['record'].keys():
                    assert expected['record'][k] == r['record'][
                        k], 'Failed to compare key: {}, expected: {}, actual: {}'.format(
                            k, expected['record'][k], r['record'][k])
            except AssertionError as e:
                print('TEST FAILED')
                print('INPUT MESSAGE:\n{}'.format(r))
                raise e
        mock_ctx.get_xpu_id.assert_called_once()


if __name__ == '__main__':
    unittest.main()
