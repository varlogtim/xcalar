import logging
import logging.config
import os
import sys
import unittest
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
from test_kafka_dist import Distributer    # NOQA #nopep8
from xcalar.solutions.controller import Controller

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')
logging.config.fileConfig(os.path.dirname(Path(__file__)) + '/logging.conf')

kafka_utils = SingletonKafkaUtils.getInstance()


class TestSnapshot(unittest.TestCase):
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
                prefix='test_snapshot_properties',
                suffix='.json',
                delete=False,
            )
            json.dump(props, temp_file)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            return temp_file.name

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

        # fixme: what is tested here? new refiners go into vgorhe_refiner_export, why opic name should be unique? WHy 3 unique topic names?
        self.ref_name = self.controller.universe.universe['all_counts_params'][
            'sink']['path'].split('/')[-1]
        kafka_utils.publishToKafka(1000)

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
        kafka_utils.cleanup()

    def processDeltas(self, expected_df):
        self.controller.to_streaming()
        self.assertDfs(expected_df)

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

    def getOffsetsFull(self):
        a = self.controller.orchestrator.getAnalyzer()
        o_tbl = self.controller.orchestrator.offset_manager.getDatasetCounts(
            a.getSessionTable('$offsets').name)
        return pd.DataFrame.from_dict(o_tbl)

    def snapshot(self):
        self.controller.orchestrator.snapshot(self.controller.universeId)

    def recover(self):
        self.controller.recoverFromLatestSnapshot()

    def deleteAllTables(self):
        for t in self.controller.orchestrator.dispatcher.session.list_tables():
            if t.is_pinned():
                t.unpin()
            t.drop(delete_completely=True)

    def test_snapshot(self):
        # initial load

        self.controller.to_materializing()
        self.shared_controller.to_materializing()
        self.assertEquals(self.controller.orchestrator.watermark.low_batch_id,
                          0)

        # self.controller.snapshot() # Materializing already takes a snapshot

        self.deleteAllTables()

        old_watermark = self.controller.orchestrator.watermark

        del self.controller.orchestrator

        self.controller.recoverFromLatestSnapshot()

        self.assertEquals(self.controller.orchestrator.watermark.low_batch_id,
                          0)
        a = self.controller.orchestrator.getAnalyzer()
        records = self.controller.orchestrator.offset_manager.getDatasetCounts(
            a.getSessionTable('$offsets').name)
        for r in records:
            if r['dataset'] == 'ds2':
                assert r['max_batch_id'] == 1 and r['count'] == 495

        new_watermark = self.controller.orchestrator.watermark

        self.assertEqual(old_watermark.low_batch_id,
                         new_watermark.low_batch_id)
        self.assertEqual(old_watermark.batch_id, new_watermark.batch_id)
        self.assertEqual(old_watermark.batchInfo, new_watermark.batchInfo)

        self.shared_controller.to_streaming()
        self.controller.to_streaming()
        a = self.controller.orchestrator.getAnalyzer()
        records = self.controller.orchestrator.offset_manager.getDatasetCounts(
            a.getSessionTable('$offsets').name)
        for r in records:
            if r['dataset'] == 'ds2':
                assert r['max_batch_id'] == 2 and r['count'] == 525    # 1515
            elif r['dataset'] == 'ds2_counts':
                assert r['max_batch_id'] == 2 and r['count'] == 3
            elif r['dataset'] == 'increment_from_dataset':
                assert r['max_batch_id'] == 2 and r['count'] == 365

        # TODO: this needs to be revisited for cross-session tests. Need to compare beofire and after rather than a fixed
        # cols = ['dataset', 'max_batch_id', 'count']
        # data = [
        #     # ('ds1', 2, 495),
        #     ('ds2', 2, 525),
        #     ('ds_no_stream', 1, 1425),
        #     ('increment_from_dataset', 2, 409),
        #     ('info', 2, 5),  # 2
        #     ('all_counts_full', 2, 10),  # 6
        #     ('all_counts_params', 2, 10),
        #     ('ds1_counts', 2, 3),
        #     ('ds2_counts', 2, 3),
        # ]
        # self.processDeltas(pd.DataFrame.from_records(data, columns=cols))

        # self.assertEqual(self.controller.orchestrator.watermark.low_batch_id, 1)

        # expectedDict = {
        #     'ds1_counts': {'lowBatchId': 3},
        #     'ds2_counts': {'lowBatchId': 3},
        #     'all_counts_full': {'lowBatchId': 3},
        #     'all_counts_params': {'lowBatchId': 3},
        # }
        # actualDict = self.controller.orchestrator.watermark.apps
        # self.assertEqual(OrderedDict(expectedDict), OrderedDict(actualDict))
        # self.assertEqual({'lowBatchId': 3}, actualDict['ds2_counts'])

        self.controller.snapshot()

        self.deleteAllTables()

        old_watermark = self.controller.orchestrator.watermark

        del self.controller.orchestrator

        self.controller.recoverFromLatestSnapshot()

        new_watermark = self.controller.orchestrator.watermark

        self.assertEqual(old_watermark.low_batch_id,
                         new_watermark.low_batch_id)
        self.assertEqual(old_watermark.batch_id, new_watermark.batch_id)
        self.assertEqual(old_watermark.batchInfo, new_watermark.batchInfo)

        self.assertEqual(self.controller.orchestrator.watermark.low_batch_id,
                         1)
        # actualDict = self.controller.orchestrator.watermark.apps
        # self.assertEqual(OrderedDict(expectedDict), OrderedDict(actualDict))
        # self.assertEqual({'lowBatchId': 3}, actualDict['ds2_counts'])

        self.controller.orchestrator.cleanLastBatch()


if __name__ == '__main__':
    unittest.main()
