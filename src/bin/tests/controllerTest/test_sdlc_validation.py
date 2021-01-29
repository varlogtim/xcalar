import unittest
import os
import logging
import uuid

from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.git.git_api import GitApi
from xcalar.solutions.sdlc import SdlcManager
from xcalar.solutions.xcalar_client_utils import (
    destroy_or_create_session,
    get_client,
    get_xcalarapi,
)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] p%(process)s {%(filename)s:%(lineno)d} '
    '%(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()],
)

REMOTE_URL = "https://localhost:{}".format(os.getenv('XCE_HTTPS_PORT','8443'))
REMOTE_USERNAME = 'admin'
REMOTE_PASSWORD = 'admin'
GIT_URL = os.environ.get('GITLAB_URL', 'https://gitlab.com/api/v4')
GIT_PROJECT_ID = os.environ.get('GITLAB_PROJECT_ID', '14164315')
GIT_ACCESS_TOKEN = os.environ.get('GITLAB_ACCESS_TOKEN',
                                  'jALRsUHgQ2EC52WSg3Wv')
os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')


class TestSdlcValidation(unittest.TestCase):
    def setUp(self):
        self.sessionName = os.environ.get(
            'SESSION_NAME', f'test_sdlc_validation_{uuid.uuid1()}')
        self.client = get_client(REMOTE_URL, REMOTE_USERNAME, REMOTE_PASSWORD)
        self.xcalarApi = get_xcalarapi(REMOTE_URL, REMOTE_USERNAME,
                                       REMOTE_PASSWORD)
        self.session = destroy_or_create_session(self.client, self.sessionName)
        self.xcalarApi.setSession(self.session)
        self.gitApi = GitApi(GIT_URL, GIT_PROJECT_ID, GIT_ACCESS_TOKEN)
        self.dispatcher = UberDispatcher(
            self.client,
            session=self.session,
            dsetSeed='tmp',
            xcalarApi=self.xcalarApi,
            isDebug=True,
        )
        for param in self.client.get_config_params():
            if param["param_name"] == "XcalarRootCompletePath":
                xcalar_root = param["param_value"]

        if not os.path.exists(f'{xcalar_root}/snapshots'):
            os.mkdir(f'{xcalar_root}/snapshots', mode=0o777)

        self.dispatcher.update_or_create_datatarget('kafka-base', 'memory')
        self.dispatcher.update_or_create_datatarget(
            'snapshot_mnt', 'shared',
            {'mountpoint': f'{xcalar_root}/snapshots/'})
        self.dispatcher.update_or_create_datatarget(
            'snapshot', 'shared', {'mountpoint': f'{xcalar_root}/snapshots/'})

        self.sdlcManager = SdlcManager(self.dispatcher, self.gitApi)

    def tearDown(self):
        # test cleaning session
        for t in self.dispatcher.session.list_tables():
            if t._get_meta().pinned:
                t.unpin()
            t.drop(delete_completely=True)

        # test case to add: check that memory footprint before setup and after destro did not change.
        self.client.destroy_session(self.sessionName)

    def test_validation(self):
        workbook_name = f'SDLC_TEST_VALIDATION_{uuid.uuid1()}'
        workbook_path = (
            '/netstore/controller/workbooks/MODULAR_REFINER_TEST.xlrwb.tar.gz')
        with open(workbook_path, 'rb') as f:
            content = bytearray(f.read())
            self.client.upload_workbook(workbook_name, content)
        try:
            (succesful, errors) = self.sdlcManager.validate_dataflow(
                'all_counts_full', workbook_name=workbook_name)

            self.assertTrue(
                'Input Data/Node 2' in errors
                and errors['Input Data/Node 2'] ==
                'Node has link after execution configured, please uncheck this on the node'
            )
            self.assertTrue(
                'Input Data/Node 5' in errors
                and errors['Input Data/Node 5'] ==
                'Node has dataflowId or linkOutName configured, cannot have source'
            )
            self.assertTrue(
                'Input Data/Node 7' in errors
                and errors['Input Data/Node 7'] == 'Node has non param source')

        finally:
            try:
                self.client.destroy_workbook(workbook_name)
            except Exception as e:
                logging.warning(str(e))


if __name__ == '__main__':
    unittest.main()
