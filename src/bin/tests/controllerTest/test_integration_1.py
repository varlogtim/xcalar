import logging
import logging.config
import os
import sys
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
import json
import pandas as pd
import uuid
from kafka_utils import SingletonKafkaUtils
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.xcalar_client_utils import (
    get_or_create_session,
    get_client,
    get_xcalarapi,
)
# from collections import OrderedDict

sys.path.append(
    os.path.dirname(Path(__file__).parent) + '/udf')    # NOQA #nopep8
from test_kafka_dist import Distributer
from xcalar.solutions.controller import Controller

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')

os.environ['IS_INTEGRATION_TEST'] = 'true'
logging.config.fileConfig(os.path.dirname(Path(__file__)) + '/logging.conf')


class IntegrationTest1(unittest.TestCase):
    def generatePropertiesFromTemplate(self, url, uname, pwd, session_name,
                                       file_name):
        template_file = (f'{os.path.dirname(Path(__file__))}/{file_name}')
        with open(template_file, 'r') as f:
            props = json.load(f)
            props['xcalar']['url'] = url
            props['xcalar']['username'] = uname
            props['xcalar']['password'] = pwd
            props['xcalar']['sessionName'] = session_name
            props['git']['url'] = os.environ.get('GITLAB_URL',
                                                 props['git']['url'])
            props['git']['projectId'] = os.environ.get(
                'GITLAB_PROJECT_ID', props['git']['projectId'])
            props['git']['accessToken'] = os.environ.get(
                'GITLAB_ACCESS_TOKEN', props['git']['accessToken'])
            temp_file = NamedTemporaryFile(
                mode='w+',
                prefix='test_integration_1_properties',
                suffix='.json',
                delete=False,
            )
            json.dump(props, temp_file)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            return temp_file.name

    def hydrateUniverse(self, expected_ds1_count, expected_ds2_count):
        self.controller.materialize()
        analyzer = self.controller.orchestrator.getAnalyzer()
        print('after hydration')
        print(self.getOffsets())
        assert expected_ds1_count == analyzer.getSessionTable(
            'ds1').record_count()
        assert expected_ds2_count == analyzer.getSessionTable(
            'ds2').record_count()

    def assertDfs(self, expected_df):
        expected_df = expected_df.sort_values(
            by=['max_batch_id', 'dataset', 'count']).reset_index(drop=True)
        actual_df = self.getOffsetsFull()
        actual_df = (actual_df[[
            'dataset', 'max_batch_id', 'count'
        ]].sort_values(by=['max_batch_id', 'dataset', 'count']).reset_index(
            drop=True))
        logging.info('Expected DF:\n{}'.format(expected_df))
        logging.info('Actual DF:\n{}'.format(actual_df))
        assert expected_df.equals(actual_df)

    def processDeltas(self, controller, event_data, expected_df=None):
        controller.to_streaming(event_data=event_data)
        if expected_df is not None:
            self.assertDfs(expected_df)

    def getOffsetsFull(self):
        a = self.controller.orchestrator.getAnalyzer()
        o_tbl = self.controller.orchestrator.offset_manager.getDatasetCounts(
            a.getSessionTable('$offsets').name)
        return pd.DataFrame.from_dict(o_tbl)

    def getOffsets(self):
        analyzer = self.controller.orchestrator.getAnalyzer()
        return analyzer.toPandasDf('$offsets', 100)

    def getOffsets2(self):
        a = self.controller.orchestrator.getAnalyzer()
        offsets_tbl = self.controller.orchestrator.offset_manager.getKafkaOffsets(
            a.getSessionTable('$offsets').name)
        return pd.DataFrame.from_dict(offsets_tbl)

    def consumeOutput(self, batch_id, topic, condition=None):
        logging.info(f'===========>{topic}==========')
        msg = self.kafka_utils.consumeFromKafka(topic)
        assert msg is not None
        assert len(msg) == 1
        assert 'data' in msg[0]
        assert self.ref_name in msg[0]['data']
        assert len(msg[0]['data'][self.ref_name]) == 1
        assert msg[0]['headers']['SOURCE_BATCH_ID'] == msg[0]['data'][self.ref_name][0]['SOURCE_BATCH_ID']
        if condition:
            condition(batch_id, msg)

    def setUp(self):
        self.kafka_utils = SingletonKafkaUtils.getInstance()
        Distributer.kafka_utils = self.kafka_utils
        suffix = uuid.uuid1()    # 'test'  # - use 'test' for local tests
        shared_universe = f'test_shared_{suffix}'
        consumer_universe = f'test--{shared_universe}'
        self.sessionName = os.environ.get('SESSION_NAME', consumer_universe)
        self.shared_sessionName = os.environ.get('SESSION_NAME',
                                                 shared_universe)

        url = "https://localhost:{}".format(os.getenv('XCE_HTTPS_PORT','8443'))
        uname = 'admin'
        pwd = 'admin'
        client = get_client(url, uname, pwd)
        xcalarApi = get_xcalarapi(url, uname, pwd)
        session = get_or_create_session(client, self.shared_sessionName)

        for param in client.get_config_params():
            if param["param_name"] == "XcalarRootCompletePath":
                xcalar_root = param["param_value"]
        if not os.path.exists(f'{xcalar_root}/snapshots'):
            os.mkdir(f'{xcalar_root}/snapshots', mode=0o777)

        ud = UberDispatcher(client, session, xcalarApi)
        ud.update_or_create_datatarget('kafka-base', 'memory')
        ud.update_or_create_datatarget(
            'snapshot_mnt', 'shared',
            {'mountpoint': f'{xcalar_root}/snapshots/'})
        ud.update_or_create_datatarget(
            'snapshot', 'shared', {'mountpoint': f'{xcalar_root}/snapshots/'})

        # controller downstream from ref data session
        self.controller = Controller()
        properties_file = self.generatePropertiesFromTemplate(
            url, uname, pwd, self.sessionName,
            'test_integration_properties.json')
        self.controller.initialize(
            universeId=self.sessionName, propertyFile=properties_file)

        # ref data session controller - it will be publishing tables for other sessions to consume
        self.shared_controller = Controller()
        properties_file = self.generatePropertiesFromTemplate(
            url, uname, pwd, self.shared_sessionName,
            'test_integration_properties.json')
        self.shared_controller.initialize(
            universeId=self.shared_sessionName, propertyFile=properties_file)

        # validate that metadata did not change. if it did - other assertins in this test might need to be changed
        expected_len = 1677
        assert len(str(self.shared_controller.universe.describeTable(
            'ds1'))) == expected_len
        self.ref_name = self.controller.universe.universe['all_counts_params'][
            'sink']['path'].split('/')[-1]
        self.kafka_utils.publishToKafka(1000)

    def tearDown(self):
        a = self.controller.orchestrator.getAnalyzer()
        print(a.toPandasDf('$info', 100))
        print(a.toPandasDf('$offsets', 100))

        # test cleaning session
        for t in self.controller.orchestrator.dispatcher.session.list_tables():
            if t.is_pinned():
                t.unpin()
            t.drop(delete_completely=True)

        # test case to add: check that memory footprint before setup and after destro did not change.
        self.controller.client.destroy_session(self.sessionName)
        self.kafka_utils.cleanup()

    def verifyMaterializationTime(self):
        now = datetime.now()
        m = self.controller.orchestrator.getMaterializationTime(getMaterializationTime=True)
        for d in ['ds2', 'ds_no_stream']:
            assert d in m
            assert now.timestamp() - m[d] < 1200    # 120 breaks in debug mode

    def test_integration(self):
        # initial load
        self.controller.to_materializing()
        assert self.controller.orchestrator.watermark.low_batch_id == 0
        self.verifyMaterializationTime()
        assert self.controller.orchestrator.watermark.apps == {}

        self.shared_controller.to_materializing()
        assert self.shared_controller.orchestrator.watermark.low_batch_id == 0
        assert self.shared_controller.orchestrator.watermark.apps == {}

        # XCE has an issue here: locks are not getting delete
        # for x in range(6):
        #     try:
        #         shared_tables = self.controller.orchestrator.dispatcher.xcalarClient. \
        #             list_global_table_names()
        #         break
        #     except Exception as ex:
        #         logging.debug(f'***: {type(ex)}, {str(ex)}')
        #         if x == 5:
        #             raise (ex)

        # assert(len(shared_tables) > 0)
        # for tname in shared_tables:
        #     logging.info('Found a shared table: {}\n'.format(tname))

        # self.processDeltas(self.shared_controller, None)

        # process deltas #1
        cols = ['dataset', 'max_batch_id', 'count']
        data = [
            ('ds2', 2, 525),
            ('ds_no_stream', 1, 1425),
            ('increment_from_dataset', 2, 365),
            ('info', 2, 5),
            ('all_counts_full', 2, 4),    # 6
            ('all_counts_params', 2, 4),    # 6
            ('ds1_counts', 2, 1),    # 3
            ('ds2_counts', 2, 3)
        ]
        self.processDeltas(self.controller, {'num_runs': 1, 'sleep_between_runs': False},
                           pd.DataFrame.from_records(data, columns=cols))
        assert self.controller.orchestrator.watermark.low_batch_id == 1
        # expectedDict = {'ds1_counts': {'lowBatchId': 3}, 'ds2_counts': {'lowBatchId': 3}, 'all_counts_full': {'lowBatchId': 3}, 'all_counts_params': {'lowBatchId': 3}}
        # expectedDict = {'ds1_counts': {'self': 3, 'test_shared_test': 2}, 'ds2_counts': {'self': 3}, 'all_counts_full': {'self': 3, 'test_shared_test': 2}, 'all_counts_params': {'self': 3}}
        # actualDict = self.controller.orchestrator.watermark.apps
        # self.assertEquals(OrderedDict(expectedDict), OrderedDict(actualDict)) TODO: why asserts false?

        # TODO: reanable test for prune filter
        # assert (
        #    self.controller.orchestrator.getAnalyzer()
        #    .getSessionTable('ds2')
        #    .record_count()
        #    == 376
        # )

        # process deltas #2
        data = [
            ('ds2', 3, 555),
            ('ds_no_stream', 1, 1425),
            ('increment_from_dataset', 2, 365),
            ('info', 3, 9),
            ('all_counts_full', 2, 4),    # 6
            ('all_counts_params', 3, 14),    # 18
            ('ds1_counts', 3, 6),    # 7
            ('ds2_counts', 3, 7)
        # ("refiner_stream", 3, 1),  # reenable after refiner_2 is fixed
        ]

        self.processDeltas(self.shared_controller, {'num_runs': 2, 'sleep_between_runs': False}, None)
        self.processDeltas(self.controller, {'num_runs': 1, 'sleep_between_runs': False},
                           pd.DataFrame.from_records(data, columns=cols))
        assert self.controller.orchestrator.watermark.low_batch_id == 1
        # TODO: why 'all_counts_full': {'self': 3
        # expectedDict = {'all_counts_full': {'self': 4, 'test_shared_test': 2}, 'all_counts_params': {'self': 4}, 'ds1_counts': {'self': 4, 'test_shared_test': 4}, 'ds2_counts': {'self': 4}}
        # actualDict = self.controller.orchestrator.watermark.apps
        #  self.assertEquals(OrderedDict(expectedDict), OrderedDict(actualDict))  # TODO: what is failing here?

        # assert self.controller.orchestrator.refiners["test_refiner_2"].low_batch_id == 4 # reenable after refiner_2 is fixed
        # assert (
        #    self.controller.orchestrator.getAnalyzer()
        #    .getSessionTable('ds2')
        #    .record_count()
        #    == 377
        # )

        self.consumeOutput(3, self.kafka_utils.OUTPUT_TOPIC_1)
        self.consumeOutput(3, self.kafka_utils.OUTPUT_TOPIC_3)
        # process deltas #3
        data = [
            ('ds2', 4, 585),
            ('ds_no_stream', 1, 1425),
            ('increment_from_dataset', 2, 365),
        # ("refiner_stream", 4, 3),   # reenable after refiner_2 is fixed
        # ("info", 2, 1),  # switch after refiner_2 is fixed
            ('info', 4, 13),
            ('all_counts_full', 2, 4),    # 18
            ('all_counts_params', 4, 26),    # 34
            ('ds1_counts', 4, 9),    # 12
            ('ds2_counts', 4, 12)
        ]
        # self.processDeltas(self.shared_controller, None)
        self.processDeltas(self.controller, {'num_runs': 1, 'sleep_between_runs': False},
                           pd.DataFrame.from_records(data, columns=cols))
        assert self.controller.orchestrator.watermark.low_batch_id == 1
        # test purge/prune
        self.analyzer = self.controller.orchestrator.getAnalyzer()
        ds2 = self.analyzer.getSessionTable('ds2').name
        ds2_delta = self.controller.orchestrator.watermark.getDelta('ds2')
        base_table = self.controller.orchestrator.dispatcher.session.list_tables(
            ds2)
        delta_table = self.controller.orchestrator.dispatcher.session.list_tables(
            ds2_delta)
        base_count = base_table[0].record_count()
        delta_count = delta_table[0].record_count()
        self.processDeltas(self.controller, {'num_runs': 1, 'sleep_between_runs': False}, None)
        base_table = self.controller.orchestrator.dispatcher.session.list_tables(
            ds2)
        delta_table = self.controller.orchestrator.dispatcher.session.list_tables(
            ds2_delta)
        new_base_count = base_table[0].record_count()
        new_delta_count = delta_table[0].record_count()
        new_batch_info = self.controller.orchestrator.watermark.batchInfo
        assert new_base_count < base_count
        if min(new_batch_info.keys()) > 2:
            assert new_delta_count < delta_count
        # assert min(new_batch_info.keys()) != 1

        # TODO: this need to be revisted for the cross session shared tables support
        # expectedDict = {'ds1_counts': {'lowBatchId': 5}, 'ds2_counts': {'lowBatchId': 5}, 'all_counts_full': {'lowBatchId': 5}, 'all_counts_params': {'lowBatchId': 5}}
        # actualDict = self.controller.orchestrator.watermark.apps
        # self.assertEquals(OrderedDict(expectedDict), OrderedDict(actualDict))

        # self.consumeOutput(4, self.kafka_utils.OUTPUT_TOPIC_1)
        # fixme: what is the purpose of this assertion?
        # self.ref_name cannot be the same for ref1 and ref3
        # self.consumeOutput(4, kafka_utils.OUTPUT_TOPIC_3)
        last_max = self.getOffsets2().sort_values(
            by=['batch_id'], ascending=False).iloc[0]
        self.controller.orchestrator.adjustOffsetTable(
            self.kafka_utils.OUTPUT_TOPIC_1, '77', 3)
        current_max = self.getOffsets2().sort_values(
            by=['batch_id'], ascending=False).iloc[0]
        assert not last_max.equals(current_max)
        assert ((current_max['partition'] == 77)
                and (current_max['max_offset'] == 3)).any()

        self.controller.orchestrator.adjustUniverse(['ds_no_stream'], 'drop')
        self.controller.orchestrator.adjustUniverse(['ds_no_stream'], 'add')
        #test add/drop multiple tables
        self.controller.orchestrator.adjustUniverse(['ds2', 'ds_no_stream'], 'drop')
        self.controller.orchestrator.adjustUniverse(['ds2', 'ds_no_stream'], 'add')
        self.controller.orchestrator.adjustUniverse(['ds2'], 'replace')
        data = [
            ('ds2', 7, 1080),
            ('-', 5, 0),
            ('ds_no_stream', 1, 1425),
            ('increment_from_dataset', 2, 365),
            ('info', 4, 16),    # 7
            ('all_counts_full', 2, 6),    # 4, 18
            ('all_counts_params', 4, 26),    # 34
            ('ds1_counts', 4, 9),
            ('ds2_counts', 4, 12)
        ]
        # TODO: reenable when actual counts confirmed
        # self.assertDfs(pd.DataFrame.from_records(data, columns=cols))

        self.controller.orchestrator.cleanLastBatch()


if __name__ == '__main__':
    unittest.main()
