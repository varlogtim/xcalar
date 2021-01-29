import argparse
from IPython.testing.globalipapp import get_ipython
import json
import os

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')


def readArgs():
    parser = argparse.ArgumentParser(
        description='Auto start/stop/materialize/.. for controlelr app')
    parser.add_argument(
        '-url',
        dest='url',
        required=True,
        type=str,
        action='store',
        help="host of the Xcalar cluster",
    )
    parser.add_argument(
        '-u',
        dest='username',
        required=True,
        type=str,
        action='store',
        help="username of the Xcalar cluster",
    )
    parser.add_argument(
        '--property_path',
        dest='property_file',
        required=True,
        type=str,
        action='store',
        help="local properties_file for controller app",
    )
    parser.add_argument(
        '--universe_path',
        dest='universe_file',
        required=True,
        type=str,
        action='store',
        help="file path for universe names")
    parser.add_argument(
        '-a',
        dest='action',
        required=True,
        type=str,
        action='store',
        help="action for controller app",
    )
    parser.add_argument(
        '-t',
        dest='time_interval',
        required=False,
        default=10,
        type=int,
        action='store',
        help='time interval (seconds) betweem checks, default value is 10s'
    )
    parser.add_argument(
        '-q',
        dest='query',
        required=False,
        type=str,
        action='store',
        help="sql query"
    )

    return parser.parse_args()


def controller_action(shell, universe_names, pargs):
    url = pargs.url
    username = pargs.username
    actions = pargs.action
    time_interval = pargs.time_interval
    action_list =[]
    for action in actions:
        action_list.append(' '.join(action))
    property_file = pargs.property_file
    password = pargs.password
    shell.run_line_magic(
        magic_name='connect', line=f'-url {url} -u {username} -p {password}')
    for action in action_list:
        for universe in universe_names:
            shell.run_line_magic(
                magic_name='use',
                line=f'-session_id {universe} -properties {property_file}')
            shell.run_line_magic(magic_name='controller_app', line='-init')
            if check_init_state(shell):
                shell.run_line_magic(magic_name='controller_app', line='-stop')
                raise Exception(f'Starting app {universe} failed')
            with capture_output() as captured:
                print(f'Starting {action}....')
                shell.run_line_magic(magic_name='controller_app', line=f'-{action}')
            if 'State transition is in progress, please check controller -state' in captured.stdout \
                or 'Current controller is already in streaming state' in captured.stdout:
                raise Exception(f'{captured.stdout}')
        for universe in universe_names:
            if action != 'stop':
                shell.run_line_magic(
                    magic_name='use',
                    line=f'-session_id {universe} -properties {property_file}')
                shell.run_line_magic(magic_name='controller_app', line='-init')
                failed = check_state(shell, time_interval)
                if failed:
                    shell.run_line_magic(magic_name='controller_app', line='-stop')
                    raise Exception(f'{action} for {universe} failed')
                else:
                    print(f'{action} for {universe} is done')

    if pargs.query:
        run_query(shell, pargs.query)

def check_init_state(shell):
    failed = False
    print('Checking the init state...')
    with capture_output() as captured:
            shell.run_line_magic(
                magic_name='controller_app', line='-state')
    print(captured.stdout)
    if 'Current controller state is failed' in captured.stdout \
        or 'Current controller state is None' in captured.stdout \
            or 'Current controller died' in captured.stdout:
        failed = True
    return failed

def check_state(shell, time_interval):
    failed = False
    while True:
        print('Checking the state...')
        with capture_output() as captured:
            shell.run_line_magic(
                magic_name='controller_app', line='-state')
        if 'Current controller state is failed' in captured.stdout\
            or 'Current controller died' in captured.stdout:
            failed = True
            break
        if 'Current controller state is streaming' in captured.stdout \
                or 'Current controller state is materialize' in captured.stdout \
                    or 'Current controller state is initialized' in captured.stdout:
            break
        time.sleep(time_interval)
    return failed

def run_query(shell, query):
    shell.run_line_magic(magic_name='sql', line=f'{query}')

if __name__ == '__main__':
    pargs = readArgs()
    password = input('Input your password for xcalar cluster: ')
    pargs.password = password
    with open(f'{pargs.universe_file}') as f:
        universes = json.load(f)
    universe_names = universes['universes']

    shell = get_ipython()
    shell.magic('load_ext xcalar.solutions.tools.extensions.connect_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.use_ext')
    shell.magic(
        'load_ext xcalar.solutions.tools.extensions.controller_app_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.sql_ext')

    controller_action(shell, universe_names, pargs)
