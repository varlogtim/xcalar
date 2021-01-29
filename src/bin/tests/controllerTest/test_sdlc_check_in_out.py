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


class TestSdlcCheckInOut(unittest.TestCase):
    def setUp(self):
        self.sessionName = os.environ.get(
            'SESSION_NAME', f'test_sdlc_check_in_out_{uuid.uuid1()}')
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

    def test_checkin(self):
        workbook_name = f'SDLC_TEST_CHECK_IN_OUT_{uuid.uuid1()}'
        workbook_path = (
            '/netstore/controller/workbooks/GIT_TEST_WORKBOOK.xlrwb.tar.gz')
        with open(workbook_path, 'rb') as f:
            content = bytearray(f.read())
            self.client.upload_workbook(workbook_name, content)
        try:
            t = self.gitApi.listTree('test', 'foo')
            refiner_id_before = next(
                filter(lambda t: t['name'] == 'refiner', t))['id']
            self.sdlcManager.checkin_all_dataflows(['refiner', 'refiner2'],
                                                   'test',
                                                   'foo',
                                                   workbook_name=workbook_name)
            t = self.gitApi.listTree('test', 'foo')
            refiner_id_after = next(
                filter(lambda t: t['name'] == 'refiner', t))['id']
            assert refiner_id_before != refiner_id_after
            self.assert_query_strings('foo/refiner')
            self.assert_query_strings('foo/refiner2')
        finally:
            try:
                self.client.destroy_workbook(workbook_name)
            except Exception as e:
                logging.warn(str(e))

    def assert_query_strings(self, name):
        t = self.gitApi.listTree('test', name)
        filenames = list(map(lambda t: t['name'], t))
        assert ((('query_string.json' in filenames) and
                 ('optimized_query_string.json' in filenames))
                or (('query_string.json' in filenames) and
                    ('optimized_query_string.json' in filenames) and
                    ('kvsvalue.json' in filenames))
                or ('kvsvalue.json' in filenames))

    def testCheckout(self):
        assert self.sdlcManager.checkout_dataflow(
            'test', 'foo/refiner2',
            f'test_sdlc_check_in_out_1_{uuid.uuid1()}') is not None
        assert self.sdlcManager.checkout_dataflow(
            'test', 'foo/refiner',
            f'test_sdlc_check_in_out_2_{uuid.uuid1()}') is not None


if __name__ == '__main__':
    unittest.main()
