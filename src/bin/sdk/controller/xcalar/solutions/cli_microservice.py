import argparse
from IPython.testing.globalipapp import get_ipython
import json
import os
import sys

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')


def readArgs():
    parser = argparse.ArgumentParser(
        description='Auto start/stop/materialize/.. for controlelr app')
    parser.add_argument(
        '-url',
        dest='url',
        required=False,
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
        '-s',
        dest='session',
        required=True,
        type=str,
        action='store',
        help="session name"
    )
    parser.add_argument(
        '-a',
        dest='actions',
        required=True,
        type=str,
        action='store',
        help="actions in json format"
    )

    return parser.parse_args()

def execute(shell, url, username, password, session, actions):
    # TODO: add some try/catch to koad/reload extension from differnt default locations 
    shell.run_line_magic(
        magic_name='connect', line=f'-url {url} -u {username} -p {password}')
    shell.run_line_magic(magic_name='use', line=f'{session}')
    if type(actions) == dict:
        actions = [actions]
    for action in actions:
        verb = action['verb']
        shell.magic(f'load_ext xcalar.solutions.tools.extensions.{verb}_ext')
        args = action['args']
        shell.run_line_magic(
            magic_name=verb, line=args)

if __name__ == '__main__':
    shell = get_ipython()
    shell.magic('load_ext xcalar.solutions.tools.extensions.connect_ext')
    shell.magic('load_ext xcalar.solutions.tools.extensions.use_ext')
    if len(sys.argv) > 1:
        pargs = readArgs()
        password = input('Input your password for xcalar cluster: ')
        pargs.password = password
        url = pargs.url
        if url is None:
            url = 'https://localhost'
        print(url)
        username = pargs.username
        actions = json.loads(pargs.actions)
        session = pargs.session
        password = pargs.password
    else:
        url = 'https://movses.xcalar.rocks'
        username = 'movses'
        actions = {"verb":'app','args':'-test_plan demo_app -args {\"SOURCE\":\"SOURCE2ML_SQLTAG_DEST#v384\"} -tname compute#123'}  
        session = 'scale-demo'
        password = 'admin'

    execute(shell, url, username, password, session, actions)
