import sys
import json
import uuid
import time

from xcalar.external.app import App
from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.external.LegacyApi.XcalarApi import XcalarApi

from xcalar.compute.services.SchemaLoad_xcrpc import SchemaLoad
from xcalar.compute.localtypes.SchemaLoad_pb2 import (AppRequest, AppResponse)

from google.protobuf.json_format import MessageToDict


session_name = 'system_workbook_xd'
client = Client(bypass_proxy=True)


def get_session(session_name):
    try:
        return client.get_session(session_name)
    except Exception:
        return client.create_session(session_name)


def get_table_records(table_name, sname=None):
    sname = session_name if not sname else sname
    session = get_session(sname)
    table = session.get_table(table_name)
    return table.records()


def get_record_count(table_name, sname=None):
    sname = session_name if not sname else sname
    session = get_session(sname)
    table = session.get_table(table_name)
    return table.record_count()


def print_kvs_file_list(schema_hash):
    kvs = client.get_workbook(session_name).kvstore
    key_pattern = f'/Load/Files/[^/]+/{schema_hash}/[0-9]+'
    limit = 5
    for ii, key in enumerate(kvs.list(key_pattern)):
        if ii >= limit:
            break
        print(f"{key}: {kvs.lookup(key)}")


def print_kvs_schema(schema_hash):
    kvs = client.get_workbook(session_name).kvstore
    key_pattern = f'/Load/Schema/[^/]+/{schema_hash}'
    limit = 5
    for ii, key in enumerate(kvs.list(key_pattern)):
        if ii >= limit:
            break
        print(f"{key}: {kvs.lookup(key)}")


def print_full_table_records(table_name):
    [print(f"{r}") for r in get_table_records(table_name)]


def print_sample_table_records(table_name, sname=None, limit=10):
    recs = get_table_records(table_name, sname)
    for ii, rec in enumerate(recs):
        if ii >= limit:
            break
        print(f"{rec}")


def execute_schema_load_app(app_in_json):
    # raises xcalar.external.exceptions.XDPException
    schema_load = SchemaLoad(client)
    app_request = AppRequest(json=app_in_json)
    resp = schema_load.appRun(app_request)
    return resp


def run_mock_test():
    print("#"*80)
    print("Starting Discover....")
    print("#"*80)

    startTime = time.time()
    # Two files with strs after the sampled ints, i.e., should produce comps
    input_serial_obj = {'JSON': {'Type' : 'LINES'}}
    path = '/xcfield/mock/prefix/'
    file_name_pattern = '*.json'
    recursive = False

    files_table_name = "FILES_TABLE"
    schema_results_table_name = "SCHEMA_RESULTS"
    schema_report_table_name = "SCHMEA_REPORT"
    final_table_name = "FINAL_TABLE"

    # Do discover_all
    discover_all_app_in = {
        'session_name': session_name,  # This session name passing is odd
        'func': 'discover_all',
        'path': path,
        'file_name_pattern': file_name_pattern,
        'recursive': recursive,
        'input_serial_json': json.dumps(input_serial_obj),
        'files_table_name': files_table_name,
        'schema_results_table_name': schema_results_table_name,
        'schema_report_table_name': schema_report_table_name
    }
    discover_all_app_in = json.dumps(discover_all_app_in)

    execute_schema_load_app(discover_all_app_in)
    totalTime = time.time() - startTime
    print("#" * 80)
    print(f"Total time for discover: {totalTime} secs")
    print("#" * 80)

    print("#" * 80)
    num_file_records = get_record_count(files_table_name)
    num_schema_results_records = get_record_count(schema_results_table_name)
    assert num_file_records == num_schema_results_records
    print(f"File table ({files_table_name}) record count matches "
          f"schema results ({schema_results_table_name}) record count")

    print("#" * 80)
    print(f"Sample of file table records ({files_table_name}):")
    print_sample_table_records(files_table_name)

    print("#" * 80)
    print(f"Sample of schema results table records ({schema_results_table_name}):")
    print_sample_table_records(schema_results_table_name)

    print("#" * 80)
    print(f"Sample of file table records ({schema_report_table_name})")
    print(f"{next(get_table_records(schema_report_table_name))}")

    rec = next(get_table_records(schema_report_table_name))

    schema_hash = rec['SCHEMA_HASH']
    startTime = time.time()
    print("#" * 80)
    print(f"Executing Dataflow Gen app for schema_hash({schema_hash})")

    get_dataflow_app_in = {
        'session_name': session_name,  # This session name passing is odd
        'func': 'get_dataflows',
        'unique_id': str(uuid.uuid4()),  # used as prefix for KVS keys
        'path': path,
        'file_name_pattern': file_name_pattern,
        'recursive': recursive,
        'schema_hash': schema_hash,
        'files_table_name': files_table_name,
        'schema_results_table_name': schema_results_table_name,
        'input_serial_json': json.dumps(input_serial_obj),
        'load_table_name': f'{final_table_name}_LOAD_TABLE',  # XD responsible for dropping
        'comp_table_name': f'{final_table_name}_COMPLIMENTS',
        'data_table_name': final_table_name
    }
    get_dataflow_app_in = json.dumps(get_dataflow_app_in)
    # XXX Ask the bois if we should have dataflow names which are the table names?

    print("#" * 80)
    app_response = execute_schema_load_app(get_dataflow_app_in)
    totalTime = time.time() - startTime
    print(f"Total time for schema hash: {totalTime} secs")
    print("#" * 80)

    app_response_json = app_response.json
    app_output = json.loads(app_response_json)
    print(f"App output object keys(): {app_output.keys()}")

    print("#" * 80)
    print(f"Listing sample Files KVS entries for hash ({schema_hash})")
    print_kvs_file_list(schema_hash)
    print("#" * 80)
    print(f"Listing sample Schema KVS entries for hash ({schema_hash})")
    print_kvs_schema(schema_hash)

    print("#" * 80)
    print("#" * 80)
    print("#" * 80)
    new_session = get_session("my_new_session")
    print(f"Got a new session: {new_session.name}")
    print(f"Copying stuff from one workbook({session_name}) to the other ({new_session.name})")
    print("")

    startTime = time.time()
    print(f"Copying Files KVS entries for hash ({schema_hash})")
    old_sess_kvs = client.get_workbook(session_name).kvstore
    new_sess_kvs = client.get_workbook(new_session.name).kvstore
    key_pattern = f'/Load/Files/[^/]+/{schema_hash}/[0-9]+'
    num_keys = 0
    for key in old_sess_kvs.list(key_pattern):
        num_keys += 1
        val = old_sess_kvs.lookup(key)
        new_sess_kvs.add_or_replace(key, val, False)
    print(f"Copied Files KVS entries - num_keys: {num_keys}")
    totalTime = time.time() - startTime
    print("#" * 80)
    print(f"Time Copying Files KVS entries for hash {totalTime}")
    print("#" * 80)

    startTime = time.time()
    print(f"Copying Schema KVS entry for hash ({schema_hash})")
    key_pattern = f'/Load/Schema/[^/]+/{schema_hash}'
    for key in old_sess_kvs.list(key_pattern):
        val = old_sess_kvs.lookup(key)
        new_sess_kvs.add_or_replace(key, val, False)
    print("Copied over schema key")
    totalTime = time.time() - startTime
    print("#" * 80)
    print(f"Time Copying Schema KVS entry for hash {totalTime}")
    print("#" * 80)

    print("#" * 80)
    print(f"Listing workbooks in session: ({session_name})")
    dataflows = client.get_workbook(session_name).list_dataflows()
    for df in dataflows:
        print(f"DATAFLOW: {df}")
    print("Retrieving dataflows:")
    (load_qs, load_qs_opt) = (app_output['load_df_query_string'],
                              app_output['load_df_optimized_query_string'])
    print(f" - Got LOAD query_strings")
    (data_qs, data_qs_opt) = (app_output['data_df_query_string'],
                              app_output['data_df_optimized_query_string'])
    print(f" - Got DATA query_strings")
    (comp_qs, comp_qs_opt) = (app_output['comp_df_query_string'],
                              app_output['comp_df_optimized_query_string'])
    print(f" - Got COMP query_strings")
    print("#" * 80)
    print("#" * 80)

    print(f"Attempting to execute dataflows in new session ({new_session.name})")
    params = {'session_name': new_session.name}

    startTime = time.time()
    # Execute LOAD
    load_table_name = f'{final_table_name}_LOAD_TABLE'
    print(f" - LOAD Dataflow, with params: {params}, to produce table: ({load_table_name})")

    load_df = Dataflow.create_dataflow_from_query_string(
        client, load_qs, optimized_query_string=load_qs_opt)

    new_session.execute_dataflow(
        load_df, table_name=load_table_name, params=params, optimized=True, is_async=False)
    totalTime = time.time() - startTime
    print("#" * 80)
    print(f"Total time for load: {totalTime} secs")
    print("#" * 80)

    startTime = time.time()
    # Execute DATA
    data_table_name = f'{final_table_name}'
    print(f" - DATA Dataflow to produce table ({data_table_name})")
    data_df = Dataflow.create_dataflow_from_query_string(
        client, data_qs, optimized_query_string=data_qs_opt)

    new_session.execute_dataflow(
        data_df, table_name=data_table_name, optimized=True, is_async=False)
    totalTime = time.time() - startTime
    print("#" * 80)
    print(f"Total time to create data table: {totalTime} secs")
    print("#" * 80)

    startTime = time.time()
    # Execute COMP
    comp_table_name = f'{final_table_name}_COMPLIMENTS'
    print(f" - COMP Dataflow to produce table ({comp_table_name})")
    comp_df = Dataflow.create_dataflow_from_query_string(
        client, comp_qs, optimized_query_string=comp_qs_opt)

    new_session.execute_dataflow(
        comp_df, table_name=comp_table_name, optimized=True, is_async=False)
    totalTime = time.time() - startTime
    print("#" * 80)
    print(f"Total time to create complement table: {totalTime} secs")
    print("#" * 80)

    print(f"Listing sample load data entries for hash ({data_table_name})")
    print_sample_table_records(data_table_name, "my_new_session")
    print("Data Table Record Count: {}".format(get_record_count(data_table_name, "my_new_session")))
    print(f"Listing sample complement load data entries for hash ({comp_table_name})")
    print_sample_table_records(comp_table_name, "my_new_session")
    print("Data Table Record Count: {}".format(get_record_count(comp_table_name, "my_new_session")))

    print("DoneAF")
    print("DoneAF")
    print("DoneAF")
    print("Let's list some tables")
    print("#" * 80)
    print("\n\t" + ("!" * 20) + "\n")
    print("\tAs a reminder: please `xclean` after each run, I am messy")
    print("\n\t" + ("!" * 20) + "\n")


"""
To run mock you must define env variables before starting xcalar. For eg
to run the memory mock test:

MOCK_TARGET=mem src/bin/usrnode/launcher.sh 1 daemon

If you would like to run the minio mock test:

MOCK_TARGET=aws src/bin/usrnode/launcher.sh 1 daemon

If MOCK_TARGET is not defined we will use it in production mode which will
be "AWS Target" for now.

Additionally define following eng:

- MOCK_NUM_FILES (default 1024)
- MOCK_NUM_COLS (max 26, default 3)
- MOCK_NUM_ROWS (default 3)
"""

run_mock_test()
