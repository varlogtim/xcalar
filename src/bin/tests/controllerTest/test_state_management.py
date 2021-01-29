import logging
import logging.config
import os
import sys
import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile
import json
from kafka_utils import SingletonKafkaUtils
import uuid
sys.path.append(
    os.path.dirname(Path(__file__).parent) + '/udf')    # NOQA #nopep8
from test_kafka_dist import Distributer
from xcalar.solutions.controller import Controller
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.xcalar_client_utils import (
    get_or_create_session,
    get_client,
    get_xcalarapi,
)

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')
print(os.path.dirname(Path(__file__)))
logging.config.fileConfig(os.path.dirname(Path(__file__)) + '/logging.conf')


class TestStateManagement(unittest.TestCase):
    def generatePropertiesFromTemplate(self, url, uname, pwd, session_name,
                                       file_name):
        template_file = (f'{os.path.dirname(Path(__file__))}/{file_name}')
        # template_file = (f'{file_name}')
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
            universeId=self.shared_sessionName, propertyFile=properties_file)

        self.controller.eventloop(num_runs=1)
        self.kafka_utils.publishToKafka(1000)

    def tearDown(self):
        try:
            if hasattr(self.controller, 'orchestrator'):
                for t in self.controller.orchestrator.dispatcher.session.list_tables(
                ):
                    if t.is_pinned():
                        t.unpin()
                    t.drop(delete_completely=True)
        except Exception:
            pass
        self.controller.client.destroy_session(self.sessionName)
        self.kafka_utils.cleanup()

    def test_state_management_1_initialize(self):
        self.assertEqual(self.controller.state, 'initialized')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'initialized')

        self.controller.persistState('materializing')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'materialized')

        self.controller.persistState('streaming')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'streaming')

        self.controller.persistState('initialized')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'initialized')

        self.controller.persistState('streaming')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'streaming')

        self.controller.persistState('snapshotting')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'snapshotting')

        self.controller.persistState('streaming')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'streaming')

        self.controller.persistState('initialized')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'initialized')

        self.controller.persistState('destroyed')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'initialized')

        self.controller.persistState('recovering')
        self.controller.eventloop(num_runs=1)
        self.assertEqual(self.controller.state, 'initialized')


if __name__ == '__main__':
    unittest.main()
