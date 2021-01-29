import argparse
from xcalar.solutions.xcalar_client_utils import get_client
from xcalar.solutions.state_persister import StatePersister
from xcalar.external.app import App
import time
import os
APP_STR_KEY = '{}_038367a9-a764-4f36-b8d2-6fa9580fcf5f'

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')


def readArgs():
    parser = argparse.ArgumentParser(description='Check App Alive and Status')
    parser.add_argument(
        '-cluster_url', dest='url', required=True, type=str, action='store')
    parser.add_argument(
        '-u', dest='username', required=True, type=str, action='store')
    parser.add_argument(
        '-p', dest='password', required=True, type=str, action='store')
    parser.add_argument(
        '-universeId',
        dest='universeId',
        required=True,
        type=str,
        action='store')
    parser.add_argument(
        '-t',
        dest='time_interval',
        required=False,
        default=100,
        type=int,
        action='store',
        help='time interval (seconds) betweem checks, default value is 100s')

    return parser.parse_args()


def check_group_id(client, universe_id):
    name = 'app_{}_id'.format(universe_id)
    result = client.global_kvstore().lookup(APP_STR_KEY.format(name))
    return result


def check(currentStatePersister, appMgr, group_id):
    if not appMgr.is_app_alive(group_id):
        raise Exception('the app is down')
    if currentStatePersister.restore_state() == 'failed':
        raise Exception('Current state is failed')


if __name__ == '__main__':
    pargs = readArgs()
    url = pargs.url
    username = pargs.username
    password = pargs.password
    universe_id = pargs.universeId
    client = get_client(url, username, password)
    try:
        group_id = check_group_id(client, universe_id)
    except Exception:
        print(f'Cannot find the record info with universe_id: {universe_id}')
        exit(1)
    appMgr = App(client)
    currentStatePersister = StatePersister(f'app_{universe_id}_current',
                                           client.global_kvstore())
    while True:
        try:
            check(currentStatePersister, appMgr, group_id)
        except Exception as e:
            print(str(e))
            exit(1)
        print('Everything looks good now')
        time.sleep(pargs.time_interval)
