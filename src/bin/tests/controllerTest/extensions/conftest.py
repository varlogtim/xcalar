import pytest
import os
import json
import sys
from IPython.testing.globalipapp import get_ipython
import atexit
from xcalar.solutions.singleton import Singleton_Controller
from xcalar.solutions.controller import exitHandler

import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile


def generatePropertiesFromTemplate():
    template_file = (os.path.dirname(Path(__file__).parent) +
                     '/test_integration_properties.json')
    with open(template_file, 'r') as f:
        props = json.load(f)
        props['xcalar']['url'] = os.environ.get('REMOTE_URL',
                                                props['xcalar']['url'])
        props['xcalar']['username'] = os.environ.get(
            'REMOTE_USERNAME', props['xcalar']['username'])
        props['xcalar']['password'] = os.environ.get(
            'REMOTE_PASSWORD', props['xcalar']['password'])
        props['xcalar']['statsFilePath'] = os.environ.get(
            'STATS_FILE_PATH', '/tmp/stats/')
        props['xcalar']['sessionName'] = os.environ.get(
            'SESSION_NAME', f'test_integration_{uuid.uuid1()}')
        props['git']['url'] = os.environ.get('GITLAB_URL', props['git']['url'])
        props['git']['projectId'] = os.environ.get('GITLAB_PROJECT_ID',
                                                   props['git']['projectId'])
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


@pytest.fixture(scope='session')
def shell():
    print('@@@@@@ setup @@@@@')


    # 1. setup environment
    os.environ[
        'XCALAR_SHELL_PROPERTIES_FILE'] = generatePropertiesFromTemplate()

    os.environ['XCALAR_SHELL_UNIVERSE_ID'] = 'var'
    os.environ['XCALAR_SHELL_SESSION_NAME'] = 'var'
    os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = 'false'
    os.environ['XCAUTOCONFIRM'] = 'yes'

    # 3. setup controller
    singleton_controller = Singleton_Controller.getInstance()
    controller = singleton_controller.get_controller()
    atexit.register(exitHandler, controller=controller)

    # 2. return ipython shell
    shell = get_ipython()
    shell.magic('load_ext xcalar.solutions.tools.extensions.session_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.controller_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.dataset_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.dataflow_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.sql_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.table_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.universe_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.workbook_ext')
    shell.magic(
        'load_ext xcalar.solutions.tools.extensions.xcalar_commands_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.xdbridge_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.sdlc_ext')

    # 4. run materialize()
    # controller.materialize(dirty=False)
    print('@@@@@@ return shell @@@@@')
    yield shell
