#!/usr/bin/env python3.6
import os
import sys
import json
import uuid
import boto3
import hashlib
import traceback

from botocore.client import Config as BotoConfig
from xcalar.compute.util.cluster import DevCluster
from xcalar.compute.util.Qa import (
    datasetCheck,
    XcalarQaS3Bucket,
    XcalarQaS3ClientArgs,
)
from xcalar.compute.services.SchemaLoad_xcrpc import SchemaLoad
from xcalar.compute.localtypes.SchemaLoad_pb2 import AppRequest
from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.external.LegacyApi.XcalarApi import (XcalarApi)
from xcalar.external.exceptions import XDPException


def debug(msg):
    sys.stderr.write(f"DEBUG: {msg}\n")
    sys.stderr.flush()


s3_prefix = 's3:/'
file_types = ['CSV', 'JSONL', 'JSON', 'PARQUET']
temp_bucket = 'xcfield'
temp_prefix = 'schema_test_script/delete_me/'

USAGE = """
Usage:
  {script} ({s3p})/path/to/file [CSV|JSONL|JSON] [OPTIONS]

Options:
  --record_delim, -r (not impl)
  --field_delim, -f (not impl)
  --use_header, -h (not impl)

Function:
  - Upload file to temp s3 location if file is local.
  - Schema Discover on file, print results tables.
  - Show query strings for loading tables.
  - Show kvstore entries for schema and file list
  - Copy kvstore entries over.
  - Execute dataflows for LOAD, DATA and COMP tables
  - Display entries for LOAD, DATA and COMP tables
""".format(script=sys.argv[0], s3p=s3_prefix)


def get_args():
    if len(sys.argv) != 3:
        print(USAGE)
        sys.exit(1)
    file_path = sys.argv[1]
    file_type = sys.argv[2].upper()
    if file_type not in file_types:
        print(f"Unknown file type: ({file_type})")
        print(USAGE)
        sys.exit(2)
    debug(f"Using: ({file_type}){file_path}")
    return (file_type, file_path)


load_wizard_session_name = "test_load_wizard"
mock_user_session_name = "someuser"


##
# Helper functions
#############################

schema_key_regex_printf = '/Load/Schema/{unique_id}/{schema_hash}'
file_list_key_regex_printf = '/Load/Files/{unique_id}/{schema_hash}/[0-9]+'


def get_schema_kvs_entry(src_workbook, unique_id, schema_hash):
    key = schema_key_regex_printf.format(
        unique_id=unique_id, schema_hash=schema_hash)
    return src_workbook.kvstore.lookup(key)


def get_files_kvs_entries(src_workbook, unique_id, schema_hash):
    key_regex = file_list_key_regex_printf.format(
        unique_id=unique_id, schema_hash=schema_hash)
    vals = []
    for key in src_workbook.kvstore.list(key_regex):
        vals.append(src_workbook.kvstore.lookup(key))
    return vals


def copy_schema_kvs_key(src_workbook, dst_workbook, unique_id, schema_hash):
    key = schema_key_regex_printf.format(
        unique_id=unique_id, schema_hash=schema_hash)
    val = src_workbook.kvstore.lookup(key)
    persist = False
    dst_workbook.kvstore.add_or_replace(key, val, persist)


def copy_files_kvs_keys(src_workbook, dst_workbook, unique_id, schema_hash):
    key_regex = file_list_key_regex_printf.format(
        unique_id=unique_id, schema_hash=schema_hash)
    persist = False
    for key in src_workbook.kvstore.list(key_regex):
        val = src_workbook.kvstore.lookup(key)
        dst_workbook.kvstore.add_or_replace(key, val, persist)


def put_object_s3(bucket, key, data):
    if not isinstance(data, bytes):
        raise ValueError("Data must be bytes")
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data)


def execute_schema_load_app(client, app_in_json):
    # raises xcalar.external.exceptions.XDPException
    schema_load = SchemaLoad(client)
    app_request = AppRequest(json=app_in_json)
    resp = schema_load.appRun(app_request)
    return resp


def handle_file_path(file_path):
    if file_path.startswith(s3_prefix):
        return file_path[len(s3_prefix):]
    debug(f"Found local file path: {file_path}")
    try:
        data = open(file_path, 'rb').read()
        file_hash = hashlib.md5(file_path.encode('utf-8')).hexdigest()
        key = f"{temp_prefix}{file_hash}"
        put_object_s3(temp_bucket, key, data)
        debug(f"Uploaded '{file_path}' to '{s3_prefix}/{temp_bucket}/{key}'")
        return f"/{temp_bucket}/{key}"
    except Exception as e:
        debug(f"Unable to process file: {file_path}: {e}")


def split_s3_file_path(file_path):
    slash = '/'
    fp = file_path.lstrip(slash)
    fp_list = fp.split(slash)
    bucket_part = fp_list[0]
    dir_part = slash.join(fp_list[1:-1]) + slash
    file_part = fp_list[-1]
    return (bucket_part, dir_part, file_part)


def get_input_serial(file_type):
    # XXX maybe support custom stuff
    csv_header_input = {'CSV': {'FileHeaderInfo': 'USE'}}
    json_lines_input = {'JSON': {'Type': 'LINES'}}
    json_doc_input = {'JSON': {'Type': 'DOCUMENT'}}
    parquet_input = {'Parquet': {}}
    if file_type == 'CSV':
        return csv_header_input
    if file_type == 'JSONL':
        return json_lines_input
    if file_type == 'JSON':
        return json_doc_input
    if file_type == 'PARQUET':
        return parquet_input
    raise ValueError("Unknown file_type({file_type})")


def run_load_wizard(client, lw_session, lw_workbook, mu_session, mu_workbook):
    unique_id = str(uuid.uuid4())

    debug(f"Executing with UUID ({unique_id}) used for all table prefixes")

    s3_file_path = handle_file_path(file_path)
    (bucket, dir_part, file_part) = split_s3_file_path(s3_file_path)
    input_serial_obj = get_input_serial(file_type)

    ##
    # Run discover_all() API on our files
    path = f'/{bucket}/{dir_part}'
    file_name_pattern = f'{file_part}'
    recursive = False
    input_serial_json = json.dumps(input_serial_obj)
    files_table_name = f"{unique_id}_FILES"
    schema_results_table_name = f"{unique_id}_SCHEMA_RESULTS"
    schema_report_table_name = f"{unique_id}_SCHEMA_REPORT"

    discover_all_app_in = {
        'session_name': lw_session.name,
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
    debug(f"Running discover_all() API: {discover_all_app_in}")

    was_success = False
    try:
        execute_schema_load_app(client, discover_all_app_in)
        debug("Success: discover_all()")
        was_success = True
    except XDPException as e:
        # So, if any one of the operations failed, we want to see what
        # the table contents are, if any, of the ones that succeeded.
        debug("ERROR: discover_all() failed: {e}")

    all_table_names = [
        files_table_name,
        schema_results_table_name,
        schema_report_table_name]
    for table_name in all_table_names:
        try:
            table = lw_session.get_table(table_name)
            print(f"Printing records for table ({table_name})")
            for rec in table.records():
                print(f"{table_name}: {rec}")
        except ValueError as e:
            print(f"Table ({table_name}) was not created")

    if not was_success:
        raise RuntimeError("Cannot continue")

    files_table = lw_session.get_table(files_table_name)
    results_table = lw_session.get_table(schema_results_table_name)
    report_table = lw_session.get_table(schema_report_table_name)

    if report_table.record_count() == 0:
        raise RuntimeError("Report table record count 0, must stop")

    report_record = next(report_table.records())
    if report_table.record_count() != 1:
        raise RuntimeError("Found more than one schema, failing")
    schema_hash = report_record['SCHEMA_HASH']
    debug(f"Using this schema_hash({schema_hash})")

    ##
    # Run get_dataflows() API with our desired schema
    kvs_unique_id = unique_id
    data_table_name = f'{unique_id}_DATA'
    load_table_name = f'{unique_id}_LOAD'
    comp_table_name = f'{unique_id}_COMP'

    get_dataflow_app_in = {
        'session_name': load_wizard_session_name,
        'func': 'get_dataflows',
        'unique_id': kvs_unique_id,
        'path': path,
        'file_name_pattern': file_name_pattern,
        'recursive': recursive,
        'schema_hash': schema_hash,
        'files_table_name': files_table_name,
        'schema_results_table_name': schema_results_table_name,
        'input_serial_json': json.dumps(input_serial_obj),
        'load_table_name': load_table_name,
        'comp_table_name': comp_table_name,
        'data_table_name': data_table_name
    }

    get_dataflow_app_in = json.dumps(get_dataflow_app_in)
    debug(f"Running get_dataflows() API: {get_dataflow_app_in}")

    app_response = execute_schema_load_app(client, get_dataflow_app_in)
    app_output = json.loads(app_response.json)
    debug(f"Success: get_dataflows() API")

    ##
    # Get Query Strings from App output
    (load_qs, load_qs_opt) = (app_output['load_df_query_string'],
                              app_output['load_df_optimized_query_string'])
    (data_qs, data_qs_opt) = (app_output['data_df_query_string'],
                              app_output['data_df_optimized_query_string'])
    (comp_qs, comp_qs_opt) = (app_output['comp_df_query_string'],
                              app_output['comp_df_optimized_query_string'])

    debug(f"LOAD QS: {load_qs}")
    #debug(f"LOAD QS OPT: {load_qs_opt}")
    debug(f"DATA QS: {data_qs}")
    #debug(f"DATA QS OPT: {data_qs_opt}")
    debug(f"COMP QS: {comp_qs}")
    #debug(f"COMP QS OPT: {comp_qs_opt}")

    ##
    # Validating KVS entries match previous tables
    results_record = next(results_table.records())
    results_schema = results_record['SCHEMA']
    kvs_schema = get_schema_kvs_entry(lw_workbook, kvs_unique_id, schema_hash)
    assert results_schema == kvs_schema

    kvs_files = get_files_kvs_entries(lw_workbook, kvs_unique_id, schema_hash)
    print(f"FILES KVS ENTRY: {kvs_files}")
    kvs_schema = get_schema_kvs_entry(lw_workbook, kvs_unique_id, schema_hash)
    print(f"SCHEMA KVS ENTRY: {kvs_schema}")

    debug("Copying KVS entries")
    copy_schema_kvs_key(lw_workbook, mu_workbook, kvs_unique_id, schema_hash)
    copy_files_kvs_keys(lw_workbook, mu_workbook, kvs_unique_id, schema_hash)
    debug(f"Successfully copied Schema and Files list to Mock User Workbook")

    ##
    # Building and executing dataflows from query strings
    debug(f"Executing LOAD dataflow")
    load_df = Dataflow.create_dataflow_from_query_string(
        client, load_qs, optimized_query_string=load_qs_opt)
    params = {'session_name': mu_session.name}
    mu_session.execute_dataflow(
        load_df, table_name=load_table_name,
        params=params, optimized=True, is_async=False)

    debug(f"Executing DATA dataflow")
    data_df = Dataflow.create_dataflow_from_query_string(
        client, data_qs, optimized_query_string=data_qs_opt)
    mu_session.execute_dataflow(
        data_df, table_name=data_table_name, optimized=True, is_async=False)

    debug(f"Executing COMP dataflow")
    comp_df = Dataflow.create_dataflow_from_query_string(
        client, comp_qs, optimized_query_string=comp_qs_opt)
    mu_session.execute_dataflow(
        comp_df, table_name=comp_table_name, optimized=True, is_async=False)

    ##
    # Validating table records and counts
    load_table = mu_session.get_table(load_table_name)
    data_table = mu_session.get_table(data_table_name)
    comp_table = mu_session.get_table(comp_table_name)

    print(f"LOAD Table contents ({load_table_name}):")
    for rec in load_table.records():
        print(f"LOAD: {rec}")

    print(f"DATA Table contents ({data_table_name}):")
    for rec in data_table.records():
        print(f"DATA: {rec}")

    print(f"COMP Table contents ({comp_table_name}):")
    for rec in comp_table.records():
        print(f"COMP: {rec}")

    return


def main(file_type, file_path):
    lw_workbook = None
    mu_workbook = None
    try:
        try:
            client = Client(bypass_proxy=True)
        except FileNotFoundError:
            debug("It doesn't look like xcalar is running...")
            raise
        # xcalar_api = XcalarApi()
        lw_workbook = client.create_workbook(load_wizard_session_name)
        lw_session = client.get_session(load_wizard_session_name)
        mu_workbook = client.create_workbook(mock_user_session_name)
        mu_session = client.get_session(mock_user_session_name)

        run_load_wizard(client, lw_session, lw_workbook, mu_session, mu_workbook)

    except Exception as e:
        debug(f"ERROR: {e}")
        debug(f"{traceback.format_exc()}")
    finally:
        if lw_workbook:
            lw_workbook.delete()
        if mu_workbook:
            mu_workbook.delete()
    return


if __name__ == "__main__":
    # execute only if run as a script
    (file_type, file_path) = get_args()
    main(file_type, file_path)
