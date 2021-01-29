import sys
import json
import pprint

from contextlib import contextmanager

from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.compute.services.SchemaLoad_xcrpc import SchemaLoad
from xcalar.compute.localtypes.SchemaLoad_pb2 import AppRequest

session_name = "FooSession"

#
# Note: this requires more work before it is ready
# Note: this requires more work before it is ready
# Note: this requires more work before it is ready
#


@contextmanager
def setup_load_wizard(user_name=None):
    client = Client(bypass_proxy=True, user_name=user_name)
    xcalar_api = XcalarApi()
    workbook = client.create_workbook(session_name)
    xcalar_api.setSession(workbook)
    session = client.get_session(session_name)

    try:
        yield (client, session, workbook)
    finally:
        workbook.delete()
        xcalar_api.setSession(None)


def execute_schema_load_app(client, app_in_json):
    # raises xcalar.external.exceptions.XDPException
    schema_load = SchemaLoad(client)
    app_request = AppRequest(json=app_in_json)
    resp = schema_load.appRun(app_request)
    return resp


def run_schema_json(data_obj):

    data_json_str = json.dumps(data_obj)

    with setup_load_wizard() as (client, session, workbook):
        app_in_obj = {
            "func": "test_get_schema_from_json",
            "session_name": session.name,
            "json_str": data_json_str
        }
        app_in_json = json.dumps(app_in_obj)
        resp = execute_schema_load_app(client, app_in_json)
        resp_obj = json.loads(resp.json)

    print("SENT THIS:")
    pprint.pprint(data_obj, indent=4)
    print("GOT THIS:")
    pprint.pprint(resp_obj, indent=4)
    return


my_obj = {
    "container": {
        "battery": {
            "type": "AAA",
            "height": 8,
            "width": 1.4
        },
        "food": {
            "type": "waffle",
            "weight": 400,
            "radius": 20
        },
        "num_items": 2,
        "item_positions": [
            [3, {"foo": 4}, 6],
        ]
    },
    "version": 1.2
}


def run_schema_parquet(path):

    target_name = 'Xcalar S3 Connector'

    with setup_load_wizard() as (client, session, workbook):
        app_in_obj = {
            "func": "test_get_schema_from_parquet",
            "session_name": session.name,
            "target_name": target_name,
            "path": path
        }
        app_in_json = json.dumps(app_in_obj)
        resp = execute_schema_load_app(client, app_in_json)
        resp_obj = json.loads(resp.json)
        pprint.pprint(resp_obj, indent=4)

    return


# run_schema_parquet(sys.argv[1])
run_schema_json(my_obj)

big_list = [
    [ii for ii in range(10)]
    for _ in range(100)
]

my_big_thing = {
    "foo": "bar",
    "big_list": big_list
}

# run_schema_json(my_big_thing)



