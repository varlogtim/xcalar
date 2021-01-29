# Importing third-party modules to faciliate data work.
from xcalar.external.client import Client
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
import json
import random
import time
import argparse
import subprocess

os.environ['XLR_PYSDK_VERIFY_SSL_CERT'] = os.environ.get(
    'XLR_PYSDK_VERIFY_SSL_CERT', 'false')
os.environ['PYTHONPATH'] = os.environ.get(
    'PYTHONPATH', '{0}/tests:{0}/udf'.format(
        os.path.dirname(Path(__file__).parent)))


def getProp(sessionName):
    template_file = (
        os.path.dirname(Path(__file__)) + '/test_cli_properties.json')
    with open(template_file, 'r') as f:
        props = json.load(f)
        props['xcalar']['sessionName'] = sessionName
        temp_file = NamedTemporaryFile(
            mode='w+',
            prefix='test_ms_properties',
            suffix='.json',
            delete=False,
        )
        json.dump(props, temp_file)
        temp_file.flush()
        os.fsync(temp_file.fileno())
        return props, temp_file.name


def recovery(sessionName, propertiesFile):
    args = [
        'python',
        './PoCs/customer4/PWMPilot2019/wheels/xcalar/solutions/controller.py',
        "recover", "--properties", propertiesFile, "--universe_id",
        sessionName, "--session", sessionName
    ]
    print(' '.join(args))
    with open(f"{sessionName}.out", "a") as f:
        subprocess.check_call(args, stdout=f, stderr=subprocess.STDOUT)
    return


def readArgs():
    parser = argparse.ArgumentParser(description='Multiple Premises')
    parser.add_argument('--time_last', type=int, action="store")
    return parser.parse_args()


if __name__ == "__main__":
    pargs = readArgs()
    time_last = pargs.incremental_last
    timeout = time.time() + 3600 * time_last
    while True:
        time.sleep(1800)
        rn = random.choice(range(12))
        sessionName = f'solution_in_use_no_deactivate_{rn}'
        props, propfn = getProp(sessionName)
        # destory session
        try:
            client = Client(
                url=props['xcalar']['url'],
                client_secrets={
                    "xiusername": props['xcalar']['username'],
                    "xipassword": props['xcalar']['password']
                },
                bypass_proxy=False,
                user_name=props['xcalar']['username'])
            session = client.get_session(sessionName)
            result = session.destroy()
            print("session " + sessionName + " killed")
        except ValueError:
            print("exception in deactivate session: " + sessionName +
                  " does not exist")
        # recover a session
        try:
            recovery(sessionName, propfn)
            print(f"{sessionName} recovered")
        except Exception as e:
            print(e)
        if time.time() > timeout:
            break
        time.sleep(1800)
