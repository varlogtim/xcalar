import json
import argparse
import uuid
import os

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.app import App

from xcalar.external.client import Client

from xcalar.compute.util.Qa import DefaultTargetName

argParser = argparse.ArgumentParser()
argParser.add_argument(
    '-H',
    dest='xcHost',
    required=False,
    default="localhost",
    type=str,
    help='hostname of the Xcalar cluster')
argParser.add_argument(
    '-a',
    dest='xcPort',
    required=False,
    default="443",
    type=str,
    help='port of the Xcalar cluster')
argParser.add_argument(
    '-U', dest="xcUser", help="username", required=False, default="admin")
argParser.add_argument(
    '-P',
    dest="xcPass",
    help="password of xcalar user",
    required=False,
    default="admin")
argParser.add_argument(
    '-s',
    dest="srcPath",
    help="source path of dataset to shard",
    required=True)
argParser.add_argument(
    '-d',
    dest="destPath",
    help="destination path on the cluster",
    required=True)
cliargs = argParser.parse_args()

appName = 'shardFiles'
session_name = uuid.uuid4().hex


def add_app(client):
    app = App(client)
    with open("shardDatasetFiles.py") as fp:
        appSrc = fp.read()
    app.set_py_app(appName, appSrc)


def shardDatasetFiles(client, input_obj):
    app = App(client)
    app.run_py_app(appName, True, json.dumps(input_obj))


if __name__ == "__main__":
    os.environ["XLR_PYSDK_VERIFY_SSL_CERT"] = "false"
    url = 'https://{}:{}'.format(cliargs.xcHost, cliargs.xcPort)
    client_secrets = {
        'xiusername': cliargs.xcUser,
        'xipassword': cliargs.xcPass
    }
    client = Client(url=url, client_secrets=client_secrets)
    xc_api = XcalarApi(url=url, client_secrets=client_secrets)
    session = None
    try:
        session = client.get_session(session_name)
    except:
        session = client.create_session(session_name)
    xc_api.setSession(session)

    try:
        add_app(client)
        # source args
        src_obj = {}
        src_obj["targetName"] = DefaultTargetName
        src_obj["path"] = cliargs.srcPath
        src_obj["recursive"] = False
        src_obj["fileNamePattern"] = '*'

        input_obj = {"sourceArgs": src_obj, "destPath": cliargs.destPath}
        shardDatasetFiles(client, input_obj)
        print("Done!")
    finally:
        session.destroy()
