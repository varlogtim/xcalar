import pytest
import os
import uuid
import json
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from IPython.utils.io import capture_output
from IPython.testing.globalipapp import get_ipython

from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.state_persister import StatePersister
from xcalar.solutions.xcalar_client_utils import (
    get_or_create_session,
    get_client,
    get_xcalarapi,
)


# For controller app test, it will launch a cluster on jenkins node
# and start the app on the launched cluster
def generateRemotePropertiesFromTemplate(file_name):
    template_file = os.path.dirname(Path(__file__)) + '/../' + file_name
    with open(template_file, 'r') as f:
        props = json.load(f)
        props['git']['url'] = os.environ.get('GITLAB_URL', props['git']['url'])
        props['git']['projectId'] = os.environ.get('GITLAB_PROJECT_ID',
                                                   props['git']['projectId'])
        props['git']['accessToken'] = os.environ.get(
            'GITLAB_ACCESS_TOKEN', props['git']['accessToken'])

        temp_file = NamedTemporaryFile(
            mode='w+',
            prefix='remote_app_properties',
            suffix='.json',
            delete=False,
        )
        json.dump(props, temp_file)
        temp_file.flush()
        os.fsync(temp_file.fileno())
        return temp_file.name


def generatePropertiesFromTemplate(file_name):
    template_file = os.path.dirname(Path(__file__)) + '/../' + file_name
    with open(template_file, 'r') as f:
        props = json.load(f)
        props['controllerApp']['appFilePath'] = os.path.dirname(
            Path(__file__).parent)
        props['git']['url'] = os.environ.get('GITLAB_URL',
                                                 props['git']['url'])
        props['git']['projectId'] = os.environ.get(
            'GITLAB_PROJECT_ID', props['git']['projectId'])
        props['git']['accessToken'] = os.environ.get(
            'GITLAB_ACCESS_TOKEN', props['git']['accessToken'])

        temp_file = NamedTemporaryFile(
            mode='w+',
            prefix='app_test_properties',
            suffix='.json',
            delete=False,
        )
        json.dump(props, temp_file)
        temp_file.flush()
        os.fsync(temp_file.fileno())
        return temp_file.name


def check_incremental_state(app_shell):
    count = 0
    while True:
        count = count + 1
        time.sleep(100)
        with capture_output() as captured:
            app_shell.run_line_magic(
                magic_name='controller_app', line='-state')
        if 'Current controller state is streaming' in captured.stdout or 'Current controller state is failed' in captured.stdout:
            break
        if count == 100:
            break
    return captured


def get_watermark(universe_id):
    client = get_client(
        os.environ.get('URL'), os.environ.get('URSERNAME'),
        os.environ.get('PWD'))
    statePersistor = StatePersister(universe_id, client.global_kvstore())
    orch = statePersistor.restore_state()
    return orch.watermark


@pytest.fixture(scope='module')
def app_shell():
    print('@@@@@@ setup app shell @@@@@')
    shell = get_ipython()
    os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = 'false'
    os.environ['XCAUTOCONFIRM'] = 'yes'
    os.environ['URL'] = "https://localhost:{}".format(os.getenv('XCE_HTTPS_PORT','8443'))
    os.environ['URSERNAME'] = 'admin'
    os.environ['PWD'] = 'admin'
    suffix = uuid.uuid1()
    shared_universe = f'test_shared_{suffix}'
    os.environ['APP_NAME_0'] = f'test_app_0--{shared_universe}'
    os.environ['APP_NAME_1'] = f'test_app_1--{shared_universe}'
    os.environ['SHARED_APP'] = shared_universe
    # 1. setup environment
    #remote_property_file = generateRemotePropertiesFromTemplate(
    #    'remote_app_properties.json')
    property_file = generatePropertiesFromTemplate('app_test_properties.json')
    os.environ['PROPERTY_FILE'] = property_file

    client = get_client(
        os.environ.get('URL'), os.environ.get('URSERNAME'),
        os.environ.get('PWD'))
    xcalarApi = get_xcalarapi(
        os.environ.get('URL'), os.environ.get('URSERNAME'),
        os.environ.get('PWD'))
    session = get_or_create_session(client, shared_universe)

    for param in client.get_config_params():
        if param["param_name"] == "XcalarRootCompletePath":
            xcalar_root = param["param_value"]
    if not os.path.exists(f'{xcalar_root}/snapshots'):
        os.mkdir(f'{xcalar_root}/snapshots', mode=0o777)

    ud = UberDispatcher(client, session, xcalarApi)
    ud.update_or_create_datatarget('kafka-base', 'memory')
    ud.update_or_create_datatarget('snapshot_mnt', 'shared',
                                   {'mountpoint': f'{xcalar_root}/snapshots/'})
    ud.update_or_create_datatarget('snapshot', 'shared',
                                   {'mountpoint': f'{xcalar_root}/snapshots/'})

    # 2. return ipython shell
    shell.magic('load_ext xcalar.solutions.tools.extensions.connect_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.use_ext')
    shell.magic(
        'load_ext xcalar.solutions.tools.extensions.controller_app_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.sql_ext')
    shell.run_line_magic(
        magic_name='connect',
        line=f'-url {os.environ.get("URL")} -u {os.environ.get("URSERNAME")} \
            -p {os.environ.get("PWD")}')
    # 4. return shell
    print('@@@@@@ return app shell @@@@@')
    yield shell

    # 5 cleanup
    print('@@@@@@ cleanup @@@@@')
    shell.run_line_magic(magic_name='controller_app', line='-stop')
    # stop shared controller app
    shell.run_line_magic(
        magic_name='use',
        line=f'-session_id {shared_universe} -properties {property_file}')
    shell.run_line_magic(magic_name='controller_app', line='-init')
    shell.run_line_magic(magic_name='controller_app', line='-stop')


def test_shared_app_init(app_shell):
    shared_universe = os.environ.get('SHARED_APP')
    property_file = os.environ.get('PROPERTY_FILE')
    app_shell.run_line_magic(
        magic_name='use',
        line=f'-session_id {shared_universe} -properties {property_file}')
    with capture_output() as captured:
        app_shell.run_line_magic(magic_name='controller_app', line='-init')
    assert 'controller state is initialized' in captured.stdout


def test_shared_app_materialize(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-materialize')
    assert 'Current controller state is materialized\n' in captured.stdout


def test_shared_app_incremental(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-incremental')
    if 'Timeout, please manually check the state' in captured.stdout:
        captured = check_incremental_state(app_shell)
    assert 'Current controller state is streaming\n' in captured.stdout


def test_app_init(app_shell):
    app_0 = os.environ.get('APP_NAME_0')
    property_file = os.environ.get('PROPERTY_FILE')
    app_shell.run_line_magic(
        magic_name='use',
        line=f'-session_id {app_0} -properties {property_file}')
    with capture_output() as captured:
        app_shell.run_line_magic(magic_name='controller_app', line='-init')
    assert 'controller state is initialized' in captured.stdout


def test_app_materialize(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-materialize')
    assert 'Current controller state is materialized\n' in captured.stdout


def test_app_incremental(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-incremental')
    count = 0
    if 'Timeout, please manually check the state' in captured.stdout:
        captured = check_incremental_state(app_shell)
    assert 'Current controller state is streaming\n' in captured.stdout


def test_app_state(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(magic_name='controller_app', line='-state')
    assert 'Current controller state is streaming\n' in captured.stdout


def test_app_pause(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-incremental_pause')
    assert 'Current controller state is initialized\n' in captured.stdout


def test_app_snapshot_take_list(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-snapshot_list')
    old_list = captured.stdout.splitlines()
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-snapshot_take')
    assert 'Current controller state is initialized\n' in captured.stdout
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-snapshot_list')
    new_list = captured.stdout.splitlines()
    assert len(new_list) == len(old_list) + 1


def test_app_snapshot_recover(app_shell):
    app_0 = os.environ.get('APP_NAME_0')
    old_watermark = get_watermark(app_0)
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-snapshot_recover')
    assert 'Current controller state is initialized\n' in captured.stdout
    new_watermark = get_watermark(app_0)
    assert old_watermark.low_batch_id == new_watermark.low_batch_id
    assert old_watermark.batch_id == new_watermark.batch_id
    assert old_watermark.batchInfo == new_watermark.batchInfo
    assert old_watermark.apps == new_watermark.apps


def test_app_incremental_continue(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app', line='-incremental')
    count = 0
    if 'Timeout, please manually check the state' in captured.stdout:
        while True:
            count = count + 1
            time.sleep(100)
            with capture_output() as captured:
                app_shell.run_line_magic(
                    magic_name='controller_app', line='-state')
            if 'Current controller state is streaming' in captured.stdout or 'Current controller state is failed' in captured.stdout:
                break
            if count == 100:
                break
    assert 'Current controller state is streaming\n' in captured.stdout


def test_app_materialize_snapshot(app_shell):
    app_0 = os.environ.get('APP_NAME_0')
    app_1 = os.environ.get('APP_NAME_1')
    property_file = os.environ.get('PROPERTY_FILE')
    app_shell.run_line_magic(
        magic_name='use',
        line=f'-session_id {app_1} -properties {property_file}')
    app_shell.run_line_magic(magic_name='controller_app', line='-init')
    with capture_output() as captured:
        app_shell.run_line_magic(
            magic_name='controller_app',
            line=f'-materialize --using_latest_snapshot_from_universe_id '
            '{app_0}')
    assert 'Current controller state is materialized\n' in captured.stdout
    app_shell.run_line_magic(magic_name='controller_app', line='-stop')


def test_app_reset(app_shell):
    app_0 = os.environ.get('APP_NAME_0')
    property_file = os.environ.get('PROPERTY_FILE')
    app_shell.run_line_magic(
        magic_name='use',
        line=f'-session_id {app_0} -properties {property_file}')
    app_shell.run_line_magic(magic_name='controller_app', line='-init')
    with capture_output() as captured:
        app_shell.run_line_magic(magic_name='controller_app', line='-destroy')
    assert 'Current controller state is initialized\n' in captured.stdout


def test_app_stop(app_shell):
    with capture_output() as captured:
        app_shell.run_line_magic(magic_name='controller_app', line='-stop')
    assert captured.stdout == ''
