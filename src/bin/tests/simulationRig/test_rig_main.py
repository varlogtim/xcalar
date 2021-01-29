#!/usr/bin/env python

import os
import json
import time
import logging
from tempfile import NamedTemporaryFile
import argparse
from IPython.testing.globalipapp import get_ipython
from IPython.utils.io import capture_output
import concurrent.futures

from xcalar.solutions.state_persister import StatePersister
from xcalar.external.client import Client

XLRDIR = os.environ['XLRDIR']
test_plan_default_path = os.path.join(XLRDIR,
                                      'src/bin/tests/simulationRig/testPlan')

os.environ['XCAUTOCONFIRM'] = 'yes'
os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-5s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

shell = None
sleep_interval_sec = 10


def cluster_setup(args):
    if args.action == "stop":
        return
    if args.snapshotPath is None:
        raise ValueError("snapshotPath should be provided")
    # setup cluster client and session
    url = "https://{}:{}".format(args.host, args.apiport)
    username = args.uname
    password = args.password
    client = Client(
        url=url,
        client_secrets={
            "xiusername": username,
            "xipassword": password
        })

    if args.baseDataPath is None:
        for param in client.get_config_params():
            if param["param_name"] == "XcalarRootCompletePath":
                xcalar_root = param["param_value"]
        assert xcalar_root is not None
        args.baseDataPath = os.path.join(xcalar_root, "export")
        logger.warning(
            f"baseDataPath is not provided, using {args.baseDataPath} as base tables path"
        )

    # add targets
    def add_xce_connector(target_name, target_type_id, params={}):
        try:
            dt = client.get_data_target(target_name)
            dt.delete()
        except Exception:
            pass
        client.add_data_target(
            target_name=target_name,
            target_type_id=target_type_id,
            params=params)

    add_xce_connector(target_name="kafka-base", target_type_id="memory")
    add_xce_connector(
        target_name="snapshot",
        target_type_id="sharednothingsymm",
        params={"mountpoint": args.snapshotPath})
    add_xce_connector(
        target_name="data_lake_parquet",
        target_type_id="shared",
        params={"mountpoint": args.baseDataPath})


def build_app_properties(app_name, cli_args):
    test_input_props = {
        'controllerApp': {
            'stateChange': {
                'numTries': 20,
                'sleepIntervalInSec': sleep_interval_sec
            },
            'appFilePath': cli_args.appConfigPath
        },
        "git": {
            "url": "https://gitlab.com/api/v4",
            "projectId": "14164315",
            "accessToken": "jALRsUHgQ2EC52WSg3Wv"
        },
        "callbacksPluginName": "test_rig_callbacks",
        "universeAdapterPluginName": "test_rig_universe_adapter",
        'xcautoconfirm': 'yes'
    }
    xc_dict = {
        "url": "https://" + cli_args.host + ":" + str(cli_args.apiport),
        "username": cli_args.uname,
        "password": cli_args.password,
        "nodes": {},
        "sessionName": app_name
    }
    temp_file = NamedTemporaryFile(
        mode='w+',
        prefix='test_app_properties',
        suffix='.json',
        delete=False,
    )
    json.dump(test_input_props, temp_file)
    temp_file.flush()
    os.fsync(temp_file.fileno())
    return xc_dict, temp_file.name


def getWatermark(args):
    client_secrets = {
        'xiusername': args['xcalar']['username'],
        'xipassword': args['xcalar']['password']
    }
    client = Client(
        url=args['xcalar']['url'],
        client_secrets=client_secrets,
        bypass_proxy=False)
    statePersistor = StatePersister(args['sessionName'],
                                    client.global_kvstore())
    orch = statePersistor.restore_state()
    return orch.watermark


def getOffsetsFull(args, shell):
    with capture_output() as captured:
        shell.run_line_magic(
            magic_name='sql',
            line='select batch_id, dataset, count from '
            '$offsets order by dataset, batch_id')
    o = captured.stdout
    res = o.split("order by dataset, batch_id'")[-1]
    return res


def launch_shell(args):
    global shell
    if shell is None:
        shell = get_ipython()
        shell.magic('load_ext xcalar.solutions.tools.extensions.connect_ext')
        shell.magic('load_ext xcalar.solutions.tools.extensions.use_ext')
        shell.magic(
            'load_ext xcalar.solutions.tools.extensions.controller_app_ext')
        shell.magic('load_ext xcalar.solutions.tools.extensions.sql_ext')

    appName = args['sessionName']

    os.environ['XCALAR_SHELL_PROPERTIES_FILE'] = args['propertiesFile']
    os.environ['XCALAR_SHELL_UNIVERSE_ID'] = appName
    os.environ['XCALAR_SHELL_SESSION_NAME'] = appName

    shell.run_line_magic(
        magic_name='connect',
        line=f'-url {args["xcalar"]["url"]} -u {args["xcalar"]["username"]} '
        '-p {args["xcalar"]["password"]}')
    shell.run_line_magic(
        magic_name='use',
        line=f'-session_id {appName} -properties {args["propertiesFile"]}')
    return shell


def materialize(args):
    appName = args['sessionName']
    shell = launch_shell(args)
    logger.info(f' {appName} to init ')
    shell.run_line_magic(magic_name='controller_app', line='-init')
    logger.info(f' {appName} to materialize ')
    shell.run_line_magic(
        magic_name='controller_app', line=f'-materialize')
    while True:
        time.sleep(sleep_interval_sec)
        logger.info(f'checking {appName} app state')
        with capture_output() as captured:
            shell.run_line_magic(magic_name='controller_app', line='-state')
        o = captured.stdout
        if 'Current controller state is materialized' in o:
            res = f'{appName} done materialize'
            break
        if 'Current controller state is failed' in o:
            raise RuntimeError(f'{appName} failed in materialize')
    return res


def materialize_from_snapshot(args):
    appName = args['sessionName']
    shell = launch_shell(args)
    logger.info(f' {appName} to init ')
    shell.run_line_magic(magic_name='controller_app', line='-init')
    if int(appName.split('_')[-1]) == 0:
        res = f'{appName} done materialize'
        return res
    logger.info(f' {appName} to materialize ')
    shell.run_line_magic(
        magic_name='controller_app',
        line='-materialize --using_latest_snapshot_from_universe_id rig_app_0'
    )    # hardcoded here
    while True:
        time.sleep(sleep_interval_sec)
        logger.info(f'checking {appName} app state')
        with capture_output() as captured:
            shell.run_line_magic(magic_name='controller_app', line='-state')
        o = captured.stdout
        if 'Current controller state is materialized' in o:
            res = f'{appName} done materialize'
            break
        if 'Current controller state is failed' in o:
            raise RuntimeError(f'{appName} failed in materialize')
    return res


def test_incremental(input_args):
    num_runs, args = input_args
    appName = args['sessionName']
    shell = launch_shell(args)
    logger.info(f' {appName} to init ')
    shell.run_line_magic(magic_name='controller_app', line='-init')
    logger.info(f' {appName} to incremental {num_runs} ')
    shell.run_line_magic(
        magic_name='controller_app', line=f'-incremental {num_runs}')
    while True:
        time.sleep(sleep_interval_sec)
        logger.info(f'checking {appName} app state')
        with capture_output() as captured:
            shell.run_line_magic(magic_name='controller_app', line='-state')
        o = captured.stdout
        if 'Current controller state is initialized' in o:
            logger.info(f'{appName} done incremental')
            break
        if 'Current controller state is failed' in o:
            raise RuntimeError(f'{appName} failed in incremental')
    logger.info(f' get offset table of {appName}  ')
    offset_dict = getOffsetsFull(args, shell)
    logger.info(appName)
    logger.info(offset_dict)
    return offset_dict


def test_snapshot(args):
    appName = args['sessionName']
    shell = launch_shell(args)
    logger.info(f' {appName} to snapshot take ')
    old_watermark = getWatermark(args)
    shell.run_line_magic(magic_name='controller_app', line='-init')
    shell.run_line_magic(magic_name='controller_app', line='-snapshot_take')
    while True:
        time.sleep(sleep_interval_sec)
        logger.info(f'checking {appName} app state')
        with capture_output() as captured:
            shell.run_line_magic(magic_name='controller_app', line='-state')
        o = captured.stdout
        if 'Current controller state is initialized' in o or 'Current controller state is snapshotting' in o:
            logger.info(f'{appName} done snapshot')
            old_watermark = getWatermark(args)
            break
        if 'Current controller state is failed' in o:
            logger.info(f'{appName} failed in snapshot')
            raise RuntimeError(f'{appName} failed in snapshot take')
    logger.info(f' {appName} to snapshot recover ')
    shell.run_line_magic(magic_name='controller_app', line='-snapshot_recover')
    while True:
        time.sleep(sleep_interval_sec)
        logger.info(f'checking {appName} app state')
        with capture_output() as captured:
            shell.run_line_magic(magic_name='controller_app', line='-state')
        o = captured.stdout
        if 'Current controller state is initialized' in o:
            logger.info(f'{appName} done recover')
            new_watermark = getWatermark(args)
            break
        if 'Current controller state is failed' in o:
            logger.info(f'{appName} failed in recover')
            raise RuntimeError(f'{appName} failed in snapshot recover')
    # validate watermarks
    assert old_watermark.low_batch_id == new_watermark.low_batch_id
    assert old_watermark.batch_id == new_watermark.batch_id
    assert old_watermark.batchInfo == new_watermark.batchInfo
    assert old_watermark.apps == new_watermark.apps
    return True


def stop_app(args):
    appName = args['sessionName']
    shell = launch_shell(args)
    logger.info(f' {appName} to stop ')
    shell.run_line_magic(magic_name='controller_app', line='-init')
    shell.run_line_magic(magic_name='controller_app', line='-stop')
    logger.info(f' {appName} done stop ')
    return True


def parse_args():
    parser = argparse.ArgumentParser(
        description='Multiple Premises',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-a',
        dest='apiport',
        required=False,
        default=int(os.getenv('XCE_HTTPS_PORT','8443')),
        type=int,
        help='Xcalar API port (8443 for some dockers)')
    parser.add_argument(
        '-H',
        dest='host',
        required=False,
        default='localhost',
        type=str,
        help='Xcalar server hostname')
    parser.add_argument(
        '-U',
        dest='uname',
        required=False,
        default="admin",
        type=str,
        help='Xcalar username')
    parser.add_argument(
        '-P',
        dest='password',
        required=False,
        default="admin",
        type=str,
        help='Xcalar password')
    parser.add_argument(
        '--app-config',
        dest="appConfigPath",
        type=str,
        action='store',
        default=test_plan_default_path)
    parser.add_argument(
        '--num-premises',
        dest='numPremises',
        type=int,
        default=12,
        help='number of premises to run + 1 shared')
    parser.add_argument(
        '--num-incrementals',
        dest='numIncrementals',
        type=int,
        default=2,
        help='number of imd+refiner cycles to run')
    parser.add_argument(
        "--baseDataPath",
        dest="baseDataPath",
        help='base tables data directory path, defaults '
        'to export directory of xcalar root path',
        required=False)
    parser.add_argument(
        "--snapshotPath",
        dest="snapshotPath",
        help="snapshots to be written to",
        required=False)
    parser.add_argument('--action', type=str, action='store', default="all")
    return parser.parse_args()


if __name__ == '__main__':
    pargs = parse_args()
    uaction = pargs.action
    session_prefix = "rig_app"
    num_shared = 1
    num_premises = pargs.numPremises

    # cluster setup (targets)
    cluster_setup(pargs)

    # generate 1 shared session - with name ref_rig_shared
    args_shared = []
    xc_dict, properties_file = build_app_properties('ref_rig_shared', pargs)
    args_shared.append({
        'sessionName': 'ref_rig_shared',
        'propertiesFile': properties_file,
        'xcalar': xc_dict
    })
    # generate premises - with name pattern rig_app_
    args_priv = []
    for i in range(0, num_premises):
        appName = f'{session_prefix}_{i}'    # if you change this name, please change the hardcoded name on line 132 as well
        xd_dict, propertiesFile = build_app_properties(appName, pargs)
        args_priv.append({
            'sessionName': appName,
            'propertiesFile': propertiesFile,
            'xcalar': xd_dict
        })

    all_args = args_shared + args_priv
    # these are the sequence of operations we run by default
    action_seq = {
        "materialize": {
            "func":
                materialize,
            "num_workers":
                num_premises,
            "args": []
        },
        "incremental": {
            "func":
                test_incremental,
            "num_workers":
                num_premises,
            "args": [pargs.numIncrementals * 10] +
                    [pargs.numIncrementals] * num_premises
        },
        "snapshot": {
            "func": test_snapshot,
            "num_workers": num_premises,
            "args": []
        },
        "stop": {
            "func": stop_app,
            "num_workers": num_premises,
            "args": []
        }
    }
    if uaction != 'all' and set(uaction.split(',')) > set(action_seq.keys()):
        raise ValueError(
            f'Invalid action; choose from {", ".join(action_seq)}')

    for action in action_seq:
        if action in uaction or uaction == 'all':
            num_wokers = action_seq[action]['num_workers'] + 1    # shared
            func = action_seq[action]["func"]
            logger.info(f'>>>>>>>>>> Start {action} <<<<<<<<<<')
            with concurrent.futures.ProcessPoolExecutor(
                    max_workers=num_wokers) as executor:
                # run shared and private premises all in parallel
                input_args = action_seq[action]["args"]
                if input_args:
                    assert len(input_args) == len(all_args)
                    input_args = list(zip(input_args, all_args))
                else:
                    input_args = all_args
                logger.info(f'arguments: {input_args}')
                res = executor.map(func, input_args)
                success = all(out for out in res)
                if not success:
                    raise RuntimeError(f"{action} returned failure status")
            logger.info(f'>>>>>>>>>> Test {action} done <<<<<<<<<<')
        else:
            logger.warn(f'>>>>>>>>>> skipping {action} <<<<<<<<<<')
