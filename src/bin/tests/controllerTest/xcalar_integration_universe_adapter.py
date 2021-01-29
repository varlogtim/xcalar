import json
import os
from pathlib import Path
from xcalar.solutions.universe_adapter import UniverseAdapter
from kafka_utils import SingletonKafkaUtils

kafka_utils = SingletonKafkaUtils.getInstance()


class XcalarUniverseAdapter(UniverseAdapter):
    def getUniverse(self, key):

        print(f'@@getUniverse: {key} !!!')
        if key.startswith('test_shared_'):
            print(f'@@inside: shared')
            template_file = os.path.dirname(
                Path(__file__)) + '/test_integration_refdata_universe.json'
            with open(template_file, 'r') as f:
                data = json.load(f)
        elif key.startswith('test_memsql'):
            print(f'@@inside: test_memsql')
            template_file = os.path.dirname(
                Path(__file__)) + '/test_memsql_universe.json'
            with open(template_file, 'r') as f:
                data = json.load(f)
        elif key.startswith('test_mysql'):
            print(f'@@inside: test_mysql')
            template_file = os.path.dirname(
                Path(__file__)) + '/test_mysql_universe.json'
            with open(template_file, 'r') as f:
                data = json.load(f)
        else:
            # for the test purposes pass pointer to refdata unverse as part of the integration universe name
            ref_data_universe = key.split('--')[-1]
            template_file = os.path.dirname(
                Path(__file__)) + '/test_integration_universe.json'
            with open(template_file, 'r') as f:
                data = ''.join(f.readlines()) \
                       .replace('test_shared_test', ref_data_universe)
                data = json.loads(data)
            if os.getenv('IS_INTEGRATION_TEST', 'false') == 'true':
                # data['universe']['test_refiner']['sink']['path'] = f'{kafka_utils.OUTPUT_TOPIC_1}/vgorhe_refiner_export'
                # data['universe']['test_refiner_3']['sink']['path'] = f'{kafka_utils.OUTPUT_TOPIC_3}/vgorhe_test_refiner_3_export'
                data['universe']['all_counts_params']['sink'][
                    'path'] = f'{kafka_utils.OUTPUT_TOPIC_1}:COM_PARTITION/dscounts'
                data['universe']['all_counts_full']['sink'][
                    'path'] = f'{kafka_utils.OUTPUT_TOPIC_3}:0/dscounts'
        return data

    def getSchemas(self, names=[]):
        schemas = {
            'ds1_schema': {
                'columns': {
                    'col10': 'float',
                    'col11': 'int',
                    'col12': 'int',
                    'col1': 'int',
                    'col2': 'string',
                    'col3': 'string',
                    'col4': 'string',
                    'col5': 'float',
                    'col6': 'float',
                    'col7': 'string',
                    'col8': 'float',
                    'col9': 'int',
                    'kx': 'int'
                },
                'pks': ['col1', 'col2', 'kx']
            },
            'ds1_schema_cap': {
                'columns': {
                    'COL10': 'float',
                    'COL11': 'timestamp',
                    'COL12': 'timestamp',
                    'COL1': 'int',
                    'COL2': 'string',
                    'COL3': 'string',
                    'COL4': 'string',
                    'COL5': 'float',
                    'COL6': 'float',
                    'COL7': 'string',
                    'COL8': 'float',
                    'COL9': 'int'
                },
                'pks': ['COL1', 'COL2']
            },
            'ds2_schema': {
                'columns': {
                    'd1': 'int',
                    'd2': 'string',
                    'd3': 'string',
                    'd4': 'string',
        # 'd5': 'float',
        # 'd6': 'int',
                    'kx': 'int'
                },
                'pks': ['d1', 'kx']
            },
            'ds_counts_schema': {
                'columns': {
                    'SOURCE': 'string',
                    'SOURCE_BATCH_ID': 'int',
                    'CNT': 'int'
                },
                'pks': ['SOURCE_BATCH_ID', 'CNT']
            },
            'pet_schema': {
                'columns': {
                    'name': 'string',
                    'owner': 'string',
                    'species': 'string',
                    'sex': 'string',
                    'birth': 'timestamp',
                    'death': 'timestamp',
                    'xpu': 'int',
                    'log':'string'
                },
                'pks': ['name', 'owner']
            }
        }
        return {s: schema for s, schema in schemas.items() if s in names}