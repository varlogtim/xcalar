# yapf: disable
import os
import json
import uuid
import gzip
import bz2
import hashlib
import contextlib
import subprocess
import pprint
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

from contextlib import contextmanager

from xcalar.container.target.manage import get_target_data, build_target
from xcalar.compute.services.SchemaLoad_xcrpc import SchemaLoad
from xcalar.compute.localtypes.SchemaLoad_pb2 import AppRequest
from xcalar.compute.util.Qa import datasetCheck
from xcalar.container.loader.load_wizard_load_plan import (
    LoadWizardLoadPlanBuilder
)
from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.external.LegacyApi.XcalarApi import (XcalarApi)
# from xcalar.external.exceptions import XDPException

import pytest

# The thinking behind this is that mosts tests have data as an input
# and then expect certain things to appear in the data table and the
# compliments table. Also, might want to add in some expectation for
# the discover_result table. We are also testing the results of schemas
# returned after we prune them.

##
# Globals
###############################################################################
INPUT_SERIAL_JSONL = {'JSON': {'Type': 'LINES'}}
INPUT_SERIAL_JSONDOC = {'JSON': {'Type': 'DOCUMENT'}}
INPUT_SERIAL_CSV = {'CSV': {'FileHeaderInfo': 'USE'}}
INPUT_SERIAL_CSV_GZ = {'CompressionType': 'GZIP', 'CSV': {'FileHeaderInfo': 'USE'}}
INPUT_SERIAL_JSONL_GZ = {'CompressionType': 'GZIP', 'JSON': {'Type': 'LINES'}}
INPUT_SERIAL_JSONDOC_GZ = {'CompressionType': 'GZIP', 'JSON': {'Type': 'DOCUMENT'}}
INPUT_SERIAL_CSV_BZ2 = {'CompressionType': 'BZIP2', 'CSV': {'FileHeaderInfo': 'USE'}}
INPUT_SERIAL_JSONL_BZ2 = {'CompressionType': 'BZIP2', 'JSON': {'Type': 'LINES'}}
INPUT_SERIAL_JSONDOC_BZ2 = {'CompressionType': 'BZIP2', 'JSON': {'Type': 'DOCUMENT'}}
INPUT_SERIAL_PARQUET = {'Parquet': {}}

LOAD_WIZARD_SESSION_NAME = "QA_Load_Wizards"

CLOUD_TARGETS = [
    ('Xcalar S3 Connector', '/xcfield/xcalar.qa/load_wizard_temp/')  # FIXME
]

# XXX These need to be defined in a config file and
# included or queried via some API call.
ICV_COLUMN_NAME = 'XCALAR_ICV'
FRN_COLUMN_NAME = 'XCALAR_FILE_RECORD_NUM'
SRC_COLUMN_NAME = 'XCALAR_SOURCEDATA'
PATH_COLUMN_NAME = 'XCALAR_PATH'
COMPLIMENTS_COLUMN_NAMES = [
    ICV_COLUMN_NAME, FRN_COLUMN_NAME, SRC_COLUMN_NAME, PATH_COLUMN_NAME
]
ONLY_COMPLIMENTS_COLUMN_NAMES = [ICV_COLUMN_NAME, FRN_COLUMN_NAME]

GIT_DIFF_HEAD_CMD = "git diff-tree --no-commit-id --name-only -r HEAD"
RUN_TESTS_IF_GIT_DIFF_HEAD_CONTAINS = [
    "test_load_wizard.py",
    "scripts/load.py",
    "scripts/schema_discover_load.py",
    "SchemaLoad.proto",
    "container/target/manage.py",
    "container/schema_discover/",
    "container/loader/",
    "UdfPyXcalar.cpp"
]
CLOUD_TESTS_DISABLED_MSG = "No related changes and ENABLE_SLOW_TESTS not true"

##
# Fixtures
###############################################################################


# @pytest.fixture(scope="module", autouse=True)
# def minio_target():
#     client = Client(bypass_proxy=True)
#     client.add_data_target(
#         MINIO_TARGET_INFO["name"],
#         MINIO_TARGET_INFO["type_id"],
#         MINIO_TARGET_INFO["args"])
#     yield
#     client.get_data_target(MINIO_TARGET_INFO["name"]).delete()

##
#  Context Managers
###############################################################################


@contextmanager
def setup_load_wizard(user_name=None):
    try:
        client = Client(bypass_proxy=True, user_name=user_name)
        xcalar_api = XcalarApi()
        workbook = client.create_workbook(LOAD_WIZARD_SESSION_NAME)
        xcalar_api.setSession(workbook)
        session = client.get_session(LOAD_WIZARD_SESSION_NAME)

        yield (client, session, workbook)
    finally:
        try:
            workbook.delete()
            xcalar_api.setSession(None)
        except Exception:
            pass


@contextmanager
def table_context(table):
    try:
        yield table
    finally:
        try:
            print(f"Dropping table: {table.name}")
            table.drop(delete_completely=True)
        except Exception:
            pass


# XXX this is not used, and broken
@contextmanager
def loadplan_context(client, source_args, input_serial, schema, load_id):
    try:
        mod_name = f"loadplan_{str(uuid.uuid4())}"
        load_plan_builder = LoadWizardLoadPlanBuilder()
        load_plan_builder.set_schema_json(json.dumps(schema))
        load_plan_builder.set_input_serial_json(json.dumps(input_serial))
        load_plan_builder.set_load_id(load_id)
        (udf_src, func_name) = load_plan_builder.get_load_plan()

        client.create_udf_module(mod_name, udf_src)
        load_plan_full_name = f"{mod_name}:{func_name}"

        parser_fn_name = load_plan_full_name
        parser_args = {
            'loader_name': 'LoadWizardLoader',
            'loader_start_func': 'load_with_load_plan'
        }
        yield (parser_fn_name, parser_args)
    finally:
        try:
            client.get_udf_module(mod_name).delete()
        except Exception:
            pass


@contextmanager
def dataset_context(workbook, source_args, parser_fn_name, parser_args):
    try:
        dataset_name = f"dataset_{str(uuid.uuid4())}"
        dataset_builder = workbook.build_dataset(
            dataset_name,
            source_args["targetName"],
            source_args["path"],
            "udf",
            recursive=source_args["recursive"],
            file_name_pattern=source_args["fileNamePattern"],
            parser_name=parser_fn_name,
            parser_args=parser_args)
        print(f"Loading dataset({dataset_name})")
        dataset = dataset_builder.load()
        info = dataset.get_info()
        print(f"Dataset({dataset_name}) loaded")
        print(f"Dataset Info: {info}")
        yield dataset
    finally:
        try:
            dataset.delete()
        except Exception:
            pass


@contextmanager
def get_test_temp_directory(test_name,
                            target_name,
                            base_dir,
                            file_gen,
                            file_suffix):
    # Returns loadArgs->sourceArgs arguments
    paths = []
    try:

        target_data = get_target_data(target_name)
        target = build_target(target_name, target_data, base_dir)

        md5 = get_md5_prefix(test_name)
        uid = str(uuid.uuid4())

        base_dir = f"{base_dir.rstrip('/')}/"
        if not base_dir.startswith('/'):
            raise ValueError("path_prefix format: '/path/to/something'")
        unique_prefix = f"{md5}_{uid}"

        for ii, (path_part, data) in enumerate(file_gen):
            if not isinstance(data, bytes):
                raise ValueError(f"file_data must be bytes, not: {type(data)}")
            if path_part != "":
                path_part = f"{path_part.rstrip('/')}/"
            path = f"{base_dir}{unique_prefix}/{path_part}file_{ii}{file_suffix}"
            with target.open(path, 'wb') as f:
                f.write(data)
            paths.append(path)
        print(f"TEMP_DIR: BASE_DIR: {base_dir}{unique_prefix}/")
        [print(f"TEMP_DIR: PATH({ii}): {path}") for ii, path in enumerate(paths)]

        args = {"path": f"{base_dir}{unique_prefix}/",
                "fileNamePattern": f"file_*{file_suffix}",
                "recursive": True,
                "targetName": target_name,
                "paths": paths}  # Used for generating expected records
        yield args

    finally:
        for path in paths:
            try:
                target.delete(path)
                print(f"Deleted '{path}'")
            except Exception:
                pass


ENABLE_CLOUD_TESTS = None  # Managed by cloud_tests_enabled()


def cloud_tests_enabled():
    # Enable Cloud Tets based on the following conditions:
    #  - Load Wizard related files changed
    #  - ENABLE_SLOW_TESTS=true (immediate short)
    global ENABLE_CLOUD_TESTS
    if ENABLE_CLOUD_TESTS is not None:
        return ENABLE_CLOUD_TESTS

    if os.getenv("ENABLE_SLOW_TESTS", "false") == "true":
        print("Found ENABLE_SLOW_TESTS - Cloud Tests Enabled")
        ENABLE_CLOUD_TESTS = True
        return True

    git_result = subprocess.run(GIT_DIFF_HEAD_CMD.split(' '),
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    if git_result.returncode != 0:
        raise RuntimeError("Couldn't determine changes: "
                           f"CMD ({GIT_DIFF_HEAD_CMD}): "
                           f"OUT ({git_result.stdout}): "
                           f"ERR ({git_result.stderr}).")

    out_str = git_result.stdout.decode('utf-8')
    print(f"Running: '{GIT_DIFF_HEAD_CMD}'\nChanged files:")
    [print(ss) for ss in out_str.split('\n')]
    for path_part in RUN_TESTS_IF_GIT_DIFF_HEAD_CONTAINS:
        if path_part in out_str:
            print(f"Found Load Wizard Change: {path_part}"
                  " - Cloud Tests Enabled")
            ENABLE_CLOUD_TESTS = True
            return True
    print(f"Could Tests Disabled - {CLOUD_TESTS_DISABLED_MSG}")
    ENABLE_CLOUD_TESTS = False
    return False

##
# Helper functions
###############################################################################


def pp(source, obj):
    print(source)
    pprint.pprint(obj, indent=4)


#
# Api Calls
#
def execute_schema_load_app(client, app_in_json):
    # raises xcalar.external.exceptions.XDPException
    schema_load = SchemaLoad(client)
    app_request = AppRequest(json=app_in_json)
    resp = schema_load.appRun(app_request)
    return resp


def get_load_id(client, session):
    app_in = {
        "func": "get_load_id",
        "session_name": session.name,
    }
    app_in_json = json.dumps(app_in)
    app_out_raw = execute_schema_load_app(client, app_in_json)
    load_id = json.loads(app_out_raw.json)
    return load_id


# XXX fix the order of num_rows and load_id to match do_load()
def call_preview_source(
        client, session, target_name, path, input_serial_json, num_rows, load_id):
    app_in = {
        "func": "preview_rows",
        "session_name": session.name,
        "target_name": target_name,
        "path": path,
        "input_serial_json": input_serial_json,
        "num_rows": num_rows,
        "load_id": load_id
    }
    app_in_json = json.dumps(app_in)
    pp("call_preview_source(app_in):", app_in)

    app_out_raw = execute_schema_load_app(client, app_in_json)
    app_output = json.loads(app_out_raw.json)
    pp("call_preview_source(resp):", app_output)
    return app_output


def call_get_dataflows(
        client, session, source_args_json, input_serial_json, schema_json,
        load_table_name, num_rows=None):

    app_in = {
        "func": "get_dataflows_with_schema",
        "session_name": session.name,
        "source_args_json": source_args_json,
        "input_serial_json": input_serial_json,
        "schema_json": schema_json,
        "load_table_name": load_table_name,
    }
    if num_rows and isinstance(num_rows, int):
        app_in["num_rows"] = num_rows

    app_in_json = json.dumps(app_in)
    pp("call_get_dataflows(app_in)", app_in)

    app_out_raw = execute_schema_load_app(client, app_in_json)
    app_output = json.loads(app_out_raw.json)
    ret = (app_output['load_df_query_string'],
           app_output['load_df_optimized_query_string'])

    pp("call_get_dataflows(resp)", "Dataflows returned...")
    return ret


def get_md5_prefix(test_name):
    md5 = hashlib.md5(test_name.encode('utf-8')).hexdigest()
    return md5


def get_expected_rows_data_desc(data_desc, paths):
    # AWS ignores columns that are all boolean(false) when discovering schema
    # So, we need to specify the string "false" which is then converted to
    # boolean(false) when loaded.
    expt_records = [
        {nn: ff if ff != "false" else False for nn, ff in rec.items()}
        for rec in data_desc['records']]

    return expt_records

#
# Loading data
#


def do_load(client, session, source_args_json, input_serial_json,
            schema_json, load_id, num_rows=None, user_name=None):
    load_id = get_load_id(client, session)

    load_tn = f"xl_{load_id}_load"
    load_qn = f"q_{load_id}_load"

    load_dataflows = call_get_dataflows(
        client, session, source_args_json, input_serial_json, schema_json,
        load_tn, num_rows=num_rows)

    print(f"Executing LOAD dataflow.")
    load_df = Dataflow.create_dataflow_from_query_string(
        client, load_dataflows[0], optimized_query_string=load_dataflows[1])
    params = {
        'session_name': session.name,
        'user_name': user_name
    }
    # XXX hrmm...
    if params['user_name'] is None:
        params['user_name'] = "XLR_USE_DEFAULT"
    session.execute_dataflow(
        load_df,
        table_name=load_tn,
        query_name=load_qn,
        params=params,
        optimized=True,
        is_async=False)

    load_table = session.get_table(load_tn)
    return table_context(load_table)


#
# Data Validation
#


def validate_table_paths(expected_paths, paths):
    if not all([p in expected_paths for p in paths]):
        print("Expected paths:")
        [print(p) for p in expected_paths]
        print("Found paths:")
        [print(p) for p in expected_paths]
        raise ValueError("Missing paths")
    print(f"Found all file paths in {PATH_COLUMN_NAME} column")


def validate_data_table_records(expected_rows, records):
    # XXX ENG-10525, changed so that we analyzing the load
    # table instead of separate tables.
    added_columns = ['XcalarRankOver', PATH_COLUMN_NAME]
    data_records = [{nn: ff for nn, ff in rec.items()
                     if nn not in [*COMPLIMENTS_COLUMN_NAMES, 'XcalarRankOver']}
                    for rec in records if rec[ICV_COLUMN_NAME] == '']
    try:
        datasetCheck(expected_rows, data_records)
    except Exception as e:
        print("-" * 40 + "DATA RECORDS MISMATCH" + "-" * 40)
        print("Expected Records:")
        [print(f"{rec}") for rec in expected_rows]
        print("Returned Records:")
        [print(f"{rec}") for rec in data_records]
        raise


def validate_comp_table_records(expected_icv_rows, records):
    # XXX Variable names are poor... need to be consistent. Fix later
    # XXX Note, this only checks the ICV column, hrm.
    icv_records = [{nn: ff for nn, ff in rec.items()
                   if nn == ICV_COLUMN_NAME}
                   for rec in records if rec[ICV_COLUMN_NAME] != '']
    try:
        datasetCheck(expected_icv_rows, icv_records)
    except Exception as e:
        print("-" * 40 + "COMP RECORD MISMATCH" + "-" * 40)
        print("Expected ICV Records:")
        [print(f"{rec}") for rec in expected_icv_rows]
        print("Returned ICV Records:")
        [print(f"{rec}") for rec in records]
        raise


def check_icvs(expected_icvs, found_icvs):
    print("Checking ICV messages")
    [print(f"EXPECTED: {icv}") for icv in expected_icvs]
    [print(f"FOUND: {icv}") for icv in found_icvs]

    for expected_icv in expected_icvs:
        if expected_icv not in found_icvs:
            return False
    return True


def validate_load_dataset_records(expected_rows, expected_icvs, records):
    # SOURCE_DATA column is a weird ordered dict, which doesn't make sense.
    success_rows = []
    icv_rows = []
    print("Dataset Records:")
    for rec in records:
        print(f"{rec}")
        if rec[ICV_COLUMN_NAME] is not '':
            icv_rows.append(
                {nn: vv for nn, vv in rec.items()
                 if nn in COMPLIMENTS_COLUMN_NAMES}
            )
            continue
        success_rows.append(
            {nn: vv for nn, vv in rec.items()
             if nn not in COMPLIMENTS_COLUMN_NAMES}
        )
    found_icvs = []
    for rec in icv_rows:
        icv = rec.get(ICV_COLUMN_NAME)
        if icv:
            found_icvs.append(icv)

    datasetCheck(expected_rows, success_rows)
    assert(check_icvs(expected_icvs, found_icvs))


# XXX I bet that I could make a function that uses Python stack frames to determine
# the name of the variable and such that we were comparing and automatically prints
# this information if they don't match.
def compare_schema_columns(given_column, expected_column):
    # XXX Should make this do for key in expected_column.keys():
    if given_column["name"] != expected_column["name"]:
        raise ValueError(f'{given_column["name"]} != {expected_column["name"]}')
    if given_column["mapping"] != expected_column["mapping"]:
        raise ValueError(f'{given_column["mapping"]} != {expected_column["mapping"]}')
    if given_column["type"] != expected_column["type"]:
        raise ValueError(f'{given_column["type"]} != {expected_column["type"]}')
    return True


def compare_unsupported_columns(given_column, expected_column):
    if given_column["name"] != expected_column["name"]:
        raise ValueError(f'{given_column["name"]} != {expected_column["name"]}')
    if given_column["mapping"] != expected_column["mapping"]:
        raise ValueError(f'{given_column["name"]} != {expected_column["name"]}')
    if given_column["message"] != expected_column["message"]:
        raise ValueError(f'{given_column["message"]} != {expected_column["message"]}')
    return True


def compare_schemas(given_schema, expected_schema):
    if given_schema["rowpath"] != expected_schema["rowpath"]:
        raise ValueError(f"Row path mismatch: "
                         f"Found: '{given_schema['rowpath']}' "
                         f"Expected: '{expected_schema['rowpath']}'")
    for g_cols in given_schema["columns"]:
        found_match = False
        for e_cols in expected_schema["columns"]:
            try:
                found_match = compare_schema_columns(g_cols, e_cols)
                break
            except ValueError:
                pass
        if not found_match:
            raise ValueError("Columns do not match")
    return True


def compare_status(given_status, expected_status):
    if given_status["error_message"] != expected_status["error_message"]:
        raise ValueError(f"Error message does not match expected")

    # Check len
    for g_cols in given_status["unsupported_columns"]:
        found_match = False
        for e_cols in expected_status["unsupported_columns"]:
            try:
                found_match = compare_unsupported_columns(g_cols, e_cols)
                break
            except ValueError as e:
                pass
        if not found_match:
            raise ValueError("Unsupported columns do not match")
    return True


# XXX Using exceptions to handle program flow seems odd... rethink later.
# XXX Also, this function looks like garbage, fix later.
def compare_preview_responses(preview_response, expected_response):
    if expected_response["global_status"] != preview_response["global_status"]:
        msg = (f"Expected global status '{expected_response['global_status']}' "
               f"!= '{preview_response['global_status']}'")
        print(msg)
        raise ValueError(msg)

    if not len(preview_response["rows"]) == len(preview_response["schemas"]):
        msg = (f"Length of response rows({len(preview_response['rows'])})"
               f" != schemas({len(preview_response['schemas'])})")
        print(msg)
        pp("preview_response['rows']:", preview_response["rows"])
        pp("preview_response['schemas']:", preview_response["schemas"])
        raise ValueError(msg)
    if not len(preview_response["rows"]) == len(preview_response["statuses"]):
        msg = (f"Length of response rows({len(preview_response['rows'])})"
               f" != statuses({len(preview_response['statuses'])})")
        print(msg)
        pp("preview_response['rows']:", preview_response["rows"])
        pp("preview_response['statuses']:", preview_response["statuses"])
        raise ValueError(msg)

    if len(preview_response["rows"]) != len(expected_response["rows"]):
        raise ValueError(f"Length of rows list does not match expected")
    for row in expected_response['rows']:
        if row not in preview_response['rows']:
            raise ValueError(f"Missing row: {row}")

    # For each given schema, make sure we find one expected schema.
    if len(preview_response["schemas"]) != len(expected_response["schemas"]):
        msg = (f"Length of schemas list({len(preview_response['schemas'])})"
               f" does not match expected length({len(expected_response['schemas'])})")
        print(msg)
        pp("preview_response['schemas']:", preview_response["schemas"])
        pp("expected_response['schemas']:", expected_response["schemas"])
        raise ValueError(msg)
    for given_schema in preview_response["schemas"]:
        schema_found = False
        for expected_schema in expected_response["schemas"]:
            try:
                schema_found = compare_schemas(given_schema, expected_schema)
                break
            except ValueError:
                pass
        if not schema_found:
            raise ValueError("Could not find matching schema")

    # For each given status, make sure we find one expected schema
    if len(preview_response["statuses"]) != len(expected_response["statuses"]):
        raise ValueError(f"Length of statuses list does not match expected")
    for given_status in preview_response["statuses"]:
        status_found = False
        for expected_status in expected_response["statuses"]:
            try:
                status_found = compare_status(given_status, expected_status)
                break
            except ValueError:
                pass
        if not status_found:
            raise ValueError("Could not find matching status")

    return True


def print_data(data):
    print("===INPUT DATA:")
    for line in data.split(b'\n'):
        print(line)

##
# Test case checkers.
#####################


# XXX Need a better name for this.
# The input to this is a test case to validate the schema preview response
# and optionaly validate the data loaded from this schema. I am creating
# another function which will validate the data loaded but from a specified
# schema ... hrmm... perhaps this function goes away and becomes two?
def check_test_case(target_name, temp_path, test_case):
    with setup_load_wizard() as (client, session, workbook):

        load_id = get_load_id(client, session)

        path_parts = test_case.get("path_parts", [""])

        def file_gen():
            # Maybe expand to support multiple datas
            for path_part in path_parts:
                yield (path_part, test_case["data"])

        print_data(test_case['data'])

        file_ext = test_case.get("file_ext", ".ext")

        temp_dir_ctx = get_test_temp_directory(
            test_case["name"], target_name, temp_path, file_gen(), file_ext)

        with temp_dir_ctx as source_args:
            preview_file_path = source_args["paths"][0]
            preview_num_rows = test_case.get('preview_num_rows', 1)
            input_serial_json = json.dumps(test_case["input_serial"])

            ret = call_preview_source(
                client, session, target_name, preview_file_path,
                input_serial_json, preview_num_rows, load_id)

            # If our test case specifies a preview reponse, check it.
            expected_preview_resp = test_case.get("expected_preview_response")
            if expected_preview_resp is not None:
                compare_preview_responses(ret, expected_preview_resp)

            # Return here if our test case does not specify expected data.
            expected_rows = test_case.get("expected_data_rows")
            if expected_rows is None:
                return

            schema = ret["schemas"][0]
            schema_json = json.dumps(schema)
            source_args_json = json.dumps([source_args])
            load_num_rows = test_case.get("load_num_rows", 20)

            load_table_ctx = do_load(
                client, session, source_args_json, input_serial_json,
                schema_json, load_id, num_rows=load_num_rows)

            with load_table_ctx as load_table:
                records = [rec for rec in load_table.records()]
                print(f"LOAD Table Records:")
                [print(f"{rec}") for rec in records]
                validate_data_table_records(expected_rows, records)
                # XXX Expand to check compliments table


def check_test_case_specified_schema(target_name, temp_path, test_case):
    with setup_load_wizard() as (client, session, workbook):

        load_id = get_load_id(client, session)

        path_parts = test_case.get("path_parts", [""])

        def file_gen():
            # Maybe expand to support multiple datas
            for path_part in path_parts:
                yield (path_part, test_case["data"])

        print_data(test_case['data'])

        file_ext = test_case.get("file_ext", ".ext")

        # We have a temp directory context with our data in it.
        temp_dir_ctx = get_test_temp_directory(
            test_case["name"], target_name, temp_path, file_gen(), file_ext)

        with temp_dir_ctx as source_args:
            schema = test_case['schema']
            input_serial_json = json.dumps(test_case['input_serial'])
            expected_data_rows = test_case['expected_data_rows']
            expected_comp_rows = test_case['expected_comp_rows']

            schema_json = json.dumps(schema)
            source_args_json = json.dumps([source_args])
            load_num_rows = test_case.get("load_num_rows", 20)

            load_table_ctx = do_load(
                client, session, source_args_json, input_serial_json,
                schema_json, load_id, num_rows=load_num_rows)

            # We have a table context with our data in it.
            with load_table_ctx as load_table:
                records = [rec for rec in load_table.records()]
                print(f"LOAD Table Records:")
                [print(f"{rec}") for rec in records]
                validate_data_table_records(expected_data_rows, records)
                validate_comp_table_records(expected_comp_rows, records)

                # XXX This was changed to only include paths on ICVs
                # found_paths = [r[PATH_COLUMN_NAME]
                #                for r in records]
                # validate_table_paths(source_args["paths"], found_paths)
                # XXX Think more about this.


##
# Preview Response Tests
###############################

# XXX Need a test which validates that if we try to pass an array range
# in one of the mappings, that we will correctly fail. E.g., $.A[0:3]
# I don't have a framework for this type of test yet.

json_field_tests = [
    {
        'name': 'JSONL Simple - FLAT',   # noqa: W605
        'data': json.dumps({"a": 1, "b": 2}).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                '{"a":1,"b":2}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."a"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."b"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        }
    },
    {
        'name': 'JSONL Nested fields',
        'data': json.dumps({
            "a": 1,
            "b": {
                "c": 2
            }
        }).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                '{"a":1,"b":{"c":2}}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."a"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."b"."c"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        }
    },
    # XXX Python 3.6+ maintains dict insertion order.
    {
        'name': 'JSONL Nested fields with duplicate key names',
        'data': json.dumps({
            "a": {"x": 1},
            "b": {"x": 2}
        }).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                '{"a":{"x":1},"b":{"x":2}}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "X_0", "mapping": '$."a"."x"', "type": "DfInt64"},
                        {"name": "X_1", "mapping": '$."b"."x"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        }
    },
    # XXX Showing walking order of elements
    {
        'name': 'JSONL More Nested fields with duplicate key names',
        'data': json.dumps({
            "x": 1,
            "a": {
                "b": {
                    "c": {"x": 2},
                    "x": 3
                }
            }
        }).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                '{"x":1,"a":{"b":{"c":{"x":2},"x":3}}}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "X_0", "mapping": '$."x"', "type": "DfInt64"},
                        {"name": "X_1", "mapping": '$."a"."b"."c"."x"', "type": "DfInt64"},
                        {"name": "X_2", "mapping": '$."a"."b"."x"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        }
    },
    {
        'name': 'JSONL Array fields schema (now supported)',
        'data': json.dumps({
            "a": 1,
            "b": [1, 2, 3]
        }).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                '{"a":1,"b":[1,2,3]}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."a"', "type": "DfInt64"},
                        {"name": "B_0", "mapping": '$."b"[0]', "type": "DfInt64"},
                        {"name": "B_1", "mapping": '$."b"[1]', "type": "DfInt64"},
                        {"name": "B_2", "mapping": '$."b"[2]', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        }
    },
    {
        'name': 'JSON with latin-1 characters (unsupported)',
        # 'data': str('\n'.join([
        #     json.dumps({"a": "spud"}),
        #     json.dumps({"a": "spüd"})
        # ]) + '\n').encode('latin-1'),
        'data': (
            b'{"A": "spud"}\n'
            b'{"A": "sp\xFCd"}\n'
        ),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                # We error before rows are populated
            ],
            "schemas": [],
            "statuses": [],
            "global_status": {
                "error_message": (
                    'AWS ERROR: UTF-8 encoding is '
                    'required. The text encoding error '
                    'was found near byte 28.')
                # XXX FIXME This should be included
            }
        }
    },
    {
        'name': 'JSONL Types (Bool, Int, Float, Str)',
        'data': ('{'
            '"col_bool": true,'
            '"col_bool_false": false,'
            '"col_int": 1,'
            '"col_float": 2.3,'
            '"col_string": "Once upon a midnight dreary"'
        '}').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                ('{"col_bool":true,"col_bool_false":false,"col_int":1,'
                 '"col_float":2.3e0,"col_string":"Once upon a midnight dreary"}\n')
                # XXX Look into details about why 2.3 is represented as a real.
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {
                            "name": "COL_BOOL",
                            "mapping": '$."col_bool"',
                            "type": "DfBoolean"
                        },
                        {
                            "name": "COL_BOOL_FALSE",
                            "mapping": '$."col_bool_false"',
                            "type": "DfBoolean"
                        },
                        {
                            "name": "COL_INT",
                            "mapping": '$."col_int"',
                            "type": "DfInt64"
                        },
                        {
                            "name": "COL_FLOAT",
                            "mapping": '$."col_float"',
                            "type": "DfFloat64"
                        },
                        {
                            "name": "COL_STRING",
                            "mapping": '$."col_string"',
                            "type": "DfString"
                        },
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        }
    },
]

# XXX Need load test for float.


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", json_field_tests, ids=lambda case: case["name"])
def test_json_field_extraction(target_name, temp_path, test_case):

    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
    check_test_case(target_name, temp_path, test_case)


#
# XXX
# XXX
# XXX Change the tests to make them compare on the json_loads results not the strings.
# XXX
# XXX
csv_field_tests = [
    {
        'name': 'CSV - Ints, Boolean, Float, Strings',   # noqa: W605
        'data': (
            'COL_INT,COL_BOOLEAN,COL_FLOAT,COL_STRING\n'
            '1234567,false,3.14,foo\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_preview_response': {
            "rows": [
                ('{'
                 '"COL_INT":"1234567",'
                 '"COL_BOOLEAN":"false",'
                 '"COL_FLOAT":"3.14",'
                 '"COL_STRING":"foo"'
                 '}\n')
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "COL_INT", "mapping": '$."COL_INT"', "type": "DfInt64"},
                        {"name": "COL_BOOLEAN", "mapping": '$."COL_BOOLEAN"', "type": "DfBoolean"},
                        {"name": "COL_FLOAT", "mapping": '$."COL_FLOAT"', "type": "DfFloat64"},
                        {"name": "COL_STRING", "mapping": '$."COL_STRING"', "type": "DfString"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        }
    },
    {
        'name': 'CSV - String field with numbers and hyphens and dot num',
        'data': (
            'PLACE,PHONE_NUMBER,FAVORITE_DECIMAL,CUPS_OF_TEA\n'
            'London Police Department,44-20-7601-2452,.1,32000.\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_preview_response': {
            "rows": [
                ('{'
                 '"PLACE":"London Police Department",'
                 '"PHONE_NUMBER":"44-20-7601-2452",'
                 '"FAVORITE_DECIMAL":".1",'
                 '"CUPS_OF_TEA":"32000."'
                 '}\n')
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "PLACE", "mapping": '$."PLACE"', "type": "DfString"},
                        {"name": "PHONE_NUMBER", "mapping": '$."PHONE_NUMBER"', "type": "DfString"},
                        {"name": "FAVORITE_DECIMAL", "mapping": '$."FAVORITE_DECIMAL"', "type": "DfFloat64"},
                        {"name": "CUPS_OF_TEA", "mapping": '$."CUPS_OF_TEA"', "type": "DfFloat64"},
                    ]
                }
            ],
            "statuses": [{"unsupported_columns": [], "error_message": None}],
            "global_status": {"error_message": None}
        }
    },
    {
        'name': 'CSV - Trailing columns',
        'data': (
            'A,B,C\n'
            '1,2,3\n'
            '4,5,6,7,8\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'preview_num_rows': 3,
        'expected_preview_response': {
            "rows": [
                '{"A":"1","B":"2","C":"3"}\n',
                '{"A":"4","B":"5","C":"6"}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [
                {
                    "unsupported_columns": [],
                    "error_message": None
                },
                {
                    "unsupported_columns": [
                        {"name": "", "mapping": '$.""', "message": "Trailing columns detected with value(7)"},
                        {"name": "", "mapping": '$.""', "message": "Trailing columns detected with value(8)"}
                    ],
                    "error_message": None
                },
            ],
            "global_status": {
                "error_message": None
            }
        }
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", csv_field_tests, ids=lambda case: case["name"])
def test_csv_field_extraction(target_name, temp_path, test_case):

    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
    check_test_case(target_name, temp_path, test_case)


parquet_field_tests = [
    {
        'name': 'Parquet Schema Detection - all physical types',   # noqa: W605
        'path': '/xcfield/xcalar.qa/parquet/alltypes_dictionary.parquet',
        'input_serial': INPUT_SERIAL_PARQUET,
        'expected_preview_response': {
            "rows": [
                ('{"id":0,'
                '"bool_col":true,'
                '"tinyint_col":0,'
                '"smallint_col":0,'
                '"int_col":0,'
                '"bigint_col":0,'
                '"float_col":0e0,'
                '"double_col":0e0,'
                '"date_string_col":"MDEvMDEvMDk=",'
                '"string_col":"MA==",'
                '"timestamp_col":45283676094696639722160128}\n')
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "ID", "type": "DfInt64", "mapping": '$."id"'},
                        {"name": "BOOL_COL", "type": "DfBoolean", "mapping": '$."bool_col"'},
                        {"name": "TINYINT_COL", "type": "DfInt64", "mapping": '$."tinyint_col"'},
                        {"name": "SMALLINT_COL", "type": "DfInt64", "mapping": '$."smallint_col"'},
                        {"name": "INT_COL", "type": "DfInt64", "mapping": '$."int_col"'},
                        {"name": "BIGINT_COL", "type": "DfInt64", "mapping": '$."bigint_col"'},
                        {"name": "FLOAT_COL", "type": "DfFloat64", "mapping": '$."float_col"'},
                        {"name": "DOUBLE_COL", "type": "DfFloat64", "mapping": '$."double_col"'},
                        {"name": "DATE_STRING_COL", "type": "DfString", "mapping": '$."date_string_col"'},
                        {"name": "STRING_COL", "type": "DfString", "mapping": '$."string_col"'},
                        {"name": "TIMESTAMP_COL", "type": "DfString", "mapping": '$."timestamp_col"'}
                    ]
                }
            ],
            "statuses": [{"unsupported_columns": [], "error_message": None}],
            "global_status": {"error_message": None}
        }
    },
    {
        # XXX TTUCKER, generate these files dynamically. Also, we need Parquet load tests.
        'name': 'Parquet Schema Detection - Objs in lists in Objs',
        'path': '/xcfield/xcalar.qa/parquet/ttucker_generate_this.list_in_objects.parquet',
        'input_serial': INPUT_SERIAL_PARQUET,
        'expected_preview_response': {
            "rows": [
                ('{"'
                 'FLOAT_COL":[{"item":3.0884400309052145E18}],'
                 '"INT_COL":[{"item":224656887126677046}],'
                 '"DATETIME_COL":[{"item":"2020-08-26T13:14:46.000Z"}],'
                 '"STRING_COL":[{"item":"lmbreuqwwpecadtjxf"}],'
                 '"BOOLEAN_COL":[{"item":false}]'
                 '}\n')
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [  # XXX Also, you should write in checks for source type as well... will do next sprint
                        {'mapping': '$."FLOAT_COL"[0]."item"', 'name': 'ITEM_0', 'srcType': 'DOUBLE|NONE', 'type': 'DfFloat64'},
                        {'mapping': '$."INT_COL"[0]."item"', 'name': 'ITEM_1', 'srcType': 'INT64|NONE', 'type': 'DfInt64'},
                        {'mapping': '$."DATETIME_COL"[0]."item"', 'name': 'ITEM_2', 'srcType': 'INT64|TIMESTAMP_MILLIS', 'type': 'DfString'},
                        {'mapping': '$."STRING_COL"[0]."item"', 'name': 'ITEM_3', 'srcType': 'BYTE_ARRAY|UTF8', 'type': 'DfString'},
                        {'mapping': '$."BOOLEAN_COL"[0]."item"', 'name': 'ITEM_4', 'srcType': 'BOOLEAN|NONE', 'type': 'DfBoolean'}
                    ]
                }
            ],
            "statuses": [{"unsupported_columns": [], "error_message": None}],
            "global_status": {"error_message": None}
        }
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", parquet_field_tests, ids=lambda case: case["name"])
def test_parquet_field_extraction(target_name, temp_path, test_case):

    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    # This is a one off because the file is already there.
    # XXX What we really need is a way to create Parquet file data dynamically.
    # XXX look into this later...
    # check_test_case(target_name, temp_path, test_case)

    with setup_load_wizard() as (client, session, workbook):

        load_id = get_load_id(client, session)

        num_rows = 1
        input_serial_json = json.dumps(test_case["input_serial"])
        expected_response = test_case["expected_preview_response"]

        file_path = test_case["path"]
        ret = call_preview_source(
            client, session, target_name, file_path, input_serial_json, num_rows, load_id)
        compare_preview_responses(ret, expected_response)


col_name_tests = [
    {
        'name': 'JSONL Simple - FLAT',   # noqa: W605
        'data': json.dumps({"a": 1, "b": 2}).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_preview_response': {
            "rows": [
                '{"a":1,"b":2}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."a"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."b"', "type": "DfInt64"},
                    ]
                }
            ],
            "status": {
                "unsupported_columns": [],
                "error_message": None
            }
        }
    },
]


csv_load_tests = [
    {
        'name': 'CSV with protected column alias names',   # noqa: W605
        'data': ('foo,desc\n''1,2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_preview_response': {
            "rows": ['{"foo":"1","desc":"2"}\n'],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "FOO", "mapping": '$."foo"', "type": "DfInt64"},
                        {"name": "DESC", "mapping": '$."desc"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{"unsupported_columns": [], "error_message": None}],
            "global_status": {"error_message": None}
        },
        'expected_data_rows': [{"FOO": 1, "DESC": 2}]
    },
    {
        'name': 'CSV with spaces in a column name',
        'data': ('foo,foo bar\n''1,2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_preview_response': {
            "rows": [
                '{"foo":"1","foo bar":"2"}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "FOO", "mapping": '$."foo"', "type": "DfInt64"},
                        {"name": "FOO_BAR", "mapping": '$."foo bar"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        },
        'expected_data_rows': [
            {"FOO": 1, "FOO_BAR": 2}
        ]
    },
    {
        'name': 'CSV with GZIP Comppression',
        'data': gzip.compress(('foo,bar\n''1,2\n').encode('utf-8')),
        'input_serial': INPUT_SERIAL_CSV,
        'file_ext': '.gz',  # Decompression should be added by ext
        'expected_preview_response': {
            "rows": [
                '{"foo":"1","bar":"2"}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "FOO", "mapping": '$."foo"', "type": "DfInt64"},
                        {"name": "BAR", "mapping": '$."bar"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {
                "error_message": None
            }
        },
        'expected_data_rows': [
            {"FOO": 1, "BAR": 2}
        ]
    },
    {
        'name': 'CSV with BZ2 Comppression',
        'data': bz2.compress(('foo,bar\n''1,2\n').encode('utf-8')),
        'input_serial': INPUT_SERIAL_CSV,
        'file_ext': '.bz2',  # Decompression should be added by ext
        'expected_preview_response': {
            "rows": [
                '{"foo":"1","bar":"2"}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "FOO", "mapping": '$."foo"', "type": "DfInt64"},
                        {"name": "BAR", "mapping": '$."bar"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{
                "unsupported_columns": [],
                "error_message": None
            }],
            "global_status": {"error_message": None}
        },
        'expected_data_rows': [{"FOO": 1, "BAR": 2}]
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", csv_load_tests, ids=lambda case: case["name"])
def test_csv_loads(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    check_test_case(target_name, temp_path, test_case)


json_load_tests = [
    {
        'name': 'JSON with missing columns',   # noqa: W605
        'data': b'\n'.join([
            json.dumps({"A": 1, "B": 2, "C": 3}).encode('utf-8'),
            json.dumps({"A": 4, "B": 5, "C": 6}).encode('utf-8'),
            json.dumps({"A": 7, "B": 8
                # No C...
            }).encode('utf-8'),
        ]) + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 3,
        'expected_preview_response': {
            "rows": [
                '{"A":1,"B":2,"C":3}\n',
                '{"A":4,"B":5,"C":6}\n',
                '{"A":7,"B":8}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [
                {
                    "error_message": None,
                    "unsupported_columns": []
                },
                {
                    "error_message": None,
                    "unsupported_columns": []
                },
                {
                    "error_message": None,
                    "unsupported_columns": []
                },
            ],
            "global_status": {
                "error_message": None
            }
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {"A": 1, "B": 2, "C": 3},
            {"A": 4, "B": 5, "C": 6},
            {"A": 7, "B": 8},
        ]
    },
    {
        'name': 'JSON with null in column',
        'data': (
            b'{"A": 1, "B":2}\n'
            b'{"A": 3, "B":null}\n'),
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"A":1,"B":2}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                    ]
                },
            ],
            "statuses": [
                {"error_message": None, "unsupported_columns": []},
            ],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 2,
        'expected_data_rows': [
            {"A": 1, "B": 2},
            {"A": 3},
        ]
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", json_load_tests, ids=lambda case: case["name"])
def test_json_loads(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    check_test_case(target_name, temp_path, test_case)


json_array_load_tests = [
    {
        'name': 'JSON Two dimension array of ints',   # noqa: W605
        'data': json.dumps(
            {"A": [
                [1, 2, 3],
                [4, 5, 6]
            ]}
        ).encode('utf-8') + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"A":[[1,2,3],[4,5,6]]}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A_0_0", "mapping": '$."A"[0][0]', "type": "DfInt64"},
                        {"name": "A_0_1", "mapping": '$."A"[0][1]', "type": "DfInt64"},
                        {"name": "A_0_2", "mapping": '$."A"[0][2]', "type": "DfInt64"},
                        {"name": "A_1_0", "mapping": '$."A"[1][0]', "type": "DfInt64"},
                        {"name": "A_1_1", "mapping": '$."A"[1][1]', "type": "DfInt64"},
                        {"name": "A_1_2", "mapping": '$."A"[1][2]', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{"error_message": None, "unsupported_columns": []}],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 1,
        'expected_data_rows': [{
            "A_0_0": 1, "A_0_1": 2, "A_0_2": 3, "A_1_0": 4, "A_1_1": 5, "A_1_2": 6,
        }]
    },
    {
        'name': 'JSON Two dimension Array of Objects',
        'data': json.dumps(
            {"A": [
                [{"ONE": 1}, {"TWO": 2}, {"THREE": 3}],
            ]}
        ).encode('utf-8') + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"A":[[{"ONE":1},{"TWO":2},{"THREE":3}]]}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "ONE", "mapping": '$."A"[0][0]."ONE"', "type": "DfInt64"},
                        {"name": "TWO", "mapping": '$."A"[0][1]."TWO"', "type": "DfInt64"},
                        {"name": "THREE", "mapping": '$."A"[0][2]."THREE"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{"error_message": None, "unsupported_columns": []}],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 1,
        'expected_data_rows': [
            {"ONE": 1, "TWO": 2, "THREE": 3}
        ]
    },
    {
        'name': 'JSON Two dimension Array of Objects and ints',
        'data': json.dumps(
            {"A": [
                [1, {"TWO": 2}, 3],
            ]}
        ).encode('utf-8') + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"A":[[1,{"TWO":2},3]]}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A_0_0", "mapping": '$."A"[0][0]', "type": "DfInt64"},
                        {"name": "TWO", "mapping": '$."A"[0][1]."TWO"', "type": "DfInt64"},
                        {"name": "A_0_2", "mapping": '$."A"[0][2]', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{"error_message": None, "unsupported_columns": []}],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 1,
        'expected_data_rows': [
            {"A_0_0": 1, "TWO": 2, "A_0_2": 3}
        ]
    },
    {
        'name': 'JSON extremely nested Objects and lists',
        'data': json.dumps(
            {
                "A": [
                    [1, {"FOO": [2, 3]}, 4],
                    {
                        "BAR": [
                            {
                                "CAR": [
                                    {"ZAR": [5]}, 6
                                ]
                            }, 7
                        ]
                    }
                ]
            }
        ).encode('utf-8') + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"A":[[1,{"FOO":[2,3]},4],{"BAR":[{"CAR":[{"ZAR":[5]},6]},7]}]}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A_0_0", "mapping": '$."A"[0][0]', "type": "DfInt64"},
                        {"name": "FOO_0", "mapping": '$."A"[0][1]."FOO"[0]', "type": "DfInt64"},
                        {"name": "FOO_1", "mapping": '$."A"[0][1]."FOO"[1]', "type": "DfInt64"},
                        {"name": "A_0_2", "mapping": '$."A"[0][2]', "type": "DfInt64"},
                        {"name": "ZAR_0", "mapping": '$."A"[1]."BAR"[0]."CAR"[0]."ZAR"[0]', "type": "DfInt64"},
                        {"name": "CAR_1", "mapping": '$."A"[1]."BAR"[0]."CAR"[1]', "type": "DfInt64"},
                        {"name": "BAR_1", "mapping": '$."A"[1]."BAR"[1]', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{"error_message": None, "unsupported_columns": []}],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 1,
        'expected_data_rows': [
            {"A_0_0": 1, "FOO_0": 2, "FOO_1": 3, "A_0_2": 4, "ZAR_0": 5, "CAR_1": 6, "BAR_1": 7}
        ]
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", json_array_load_tests, ids=lambda case: case["name"])
def test_json_array_loads(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    check_test_case(target_name, temp_path, test_case)


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
def test_json_max_columns(target_name, temp_path):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    test_case = {
        'name': 'JSONL Test 1000 columns max',
        'data': 'GENERATE DATA',
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [],  # Generate me
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [  # Generate me
                        {"name": "GENREATE ME", "mapping": '$."A"[0][0]', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [{"error_message": 'XCE-000002E2 Max columns found for schema', "unsupported_columns": []}],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 1,
        'expected_data_rows': []  # Generate me
    }

    # Generate some data
    width = 10
    height = 100
    big_array = [
        [1 for x in range(width)]
        for y in range(height)
    ]

    # Make the data
    data_obj = {
        "my_array": big_array,
        "extra": "data that should not appear when loaded or in the schema columns"
    }
    test_case['data'] = json.dumps(data_obj).encode('utf-8') + b'\n'

    # Make the response row
    row = json.dumps(data_obj, separators=(',', ':')) + '\n'
    test_case['expected_preview_response']['rows'] = [row]

    # Make the schema:
    columns = [
        {"name": f"MY_ARRAY_{h}_{w}", "mapping": f'$."my_array"[{h}][{w}]', "type": "DfInt64"}
        for w in range(width) for h in range(height)]
    test_case['expected_preview_response']['schemas'][0]['columns'] = columns

    # Make the data rows:
    data_row = {col["name"]: 1 for col in columns}
    test_case['expected_data_rows'] = [data_row]

    check_test_case(target_name, temp_path, test_case)

# XXX Must write a lot more tests:
# XXX Must write a lot more tests:
# XXX Must write a lot more tests:
# XXX Must write a lot more tests:
# - Failure conditions for SchemaProcessors (invalid files, etc...)
# - Need to spend a lot of reading through all the code again


@pytest.mark.skip("TODO:")
@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
def test_csv_max_columns(target_name, temp_path):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    test_case = {
        'name': 'CSV Test 1000 columns max',
        'data': 'GENERATE DATA',
        'input_serial': INPUT_SERIAL_CSV,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [],  # Generate me
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [{"name": "GENREATE ME", "mapping": '$."A"', "type": "DfInt64"}]
                }
            ],
            "statuses": [{"error_message": 'XCE-000002E2 Max columns found for schema', "unsupported_columns": []}],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 1,
        'expected_data_rows': []  # Generate me
    }
    # Generate some data
    num_columns = 1001

    # Make the data
    file_header = ','.join([f"COL_{ii}" for ii in range(num_columns)]) + '\n'
    file_data = ','.join([str(ii) for ii in range(num_columns)]) + '\n'
    test_case['data'] = ''.join([file_header, file_data]).encode('utf-8')
    data_obj = {f"COL_{ii}": ii for ii in range(num_columns)}

    # Make the response row
    row = json.dumps(data_obj, separators=(',', ':')) + '\n'
    test_case['expected_preview_response']['rows'] = [row]

    # Make the schema:
    columns = [
        {"name": f"COL_{ii}", "mapping": f'$."COL_{ii}"', "type": "DfInt64"}
        for ii in range(num_columns)]
    test_case['expected_preview_response']['schemas'][0]['columns'] = columns

    # Make the data rows:
    data_row = {col["name"]: 1 for col in columns}
    test_case['expected_data_rows'] = [data_row]

    check_test_case(target_name, temp_path, test_case)


##
# Preview Response and Load Tests
#####################################


preview_load_tests = [
    {
        'name': 'Text as a CSV with space delimiter',   # noqa: W605
        'data': (
            "These are some words\n"
            "Missing a word\n"
            "But this has all\n"
        ).encode('utf-8'),
        'input_serial': {
            "CSV": {
                "FileHeaderInfo": "NONE",
                "QuoteEscapeCharacter": '"',
                "RecordDelimiter": "\n",
                "FieldDelimiter": " ",
                "QuoteCharacter": '"',
                "AllowQuotedRecordDelimiter": True}
        },
        'preview_num_rows': 3,
        'expected_preview_response': {
            # XXX It would be cool to test if selecting extra columns work
            # with HEADER: {NONE, IGNORE}
            "rows": [
                '{"_1":"These","_2":"are","_3":"some","_4":"words"}\n',
                '{"_1":"Missing","_2":"a","_3":"word"}\n',
                '{"_1":"But","_2":"this","_3":"has","_4":"all"}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "_1", "mapping": '$."_1"', "type": "DfString"},
                        {"name": "_2", "mapping": '$."_2"', "type": "DfString"},
                        {"name": "_3", "mapping": '$."_3"', "type": "DfString"},
                        {"name": "_4", "mapping": '$."_4"', "type": "DfString"},
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "_1", "mapping": '$."_1"', "type": "DfString"},
                        {"name": "_2", "mapping": '$."_2"', "type": "DfString"},
                        {"name": "_3", "mapping": '$."_3"', "type": "DfString"}
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "_1", "mapping": '$."_1"', "type": "DfString"},
                        {"name": "_2", "mapping": '$."_2"', "type": "DfString"},
                        {"name": "_3", "mapping": '$."_3"', "type": "DfString"},
                        {"name": "_4", "mapping": '$."_4"', "type": "DfString"},
                    ]
                },
            ],
            "statuses": [
                {"unsupported_columns": [], "error_message": None},
                {"unsupported_columns": [], "error_message": None},
                {"unsupported_columns": [], "error_message": None}
            ],
            "global_status": {
                "error_message": None
            }
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {"_1": "These", "_2": "are", "_3": "some", "_4": "words"},
            {"_1": "Missing", "_2": "a", "_3": "word", "_4": ""},
            {"_1": "But", "_2": "this", "_3": "has", "_4": "all"}
        ]
    },
    {
        'name': 'Text as a CSV with NO (blank) delimiter',
        'data': (
            "These are some words\n"
            "Missing a word\n"
            "But this has all\n"
        ).encode('utf-8'),
        'input_serial': {
            "CSV": {
                "FileHeaderInfo": "NONE",
                "QuoteEscapeCharacter": '"',
                "RecordDelimiter": "\n",
                "FieldDelimiter": "",  # <- Note this
                "QuoteCharacter": '"',
                "AllowQuotedRecordDelimiter": True}
        },
        'preview_num_rows': 3,
        'expected_preview_response': {
            "rows": [
                '{"_1":"These are some words"}\n',
                '{"_1":"Missing a word"}\n',
                '{"_1":"But this has all"}\n'
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "_1", "mapping": '$."_1"', "type": "DfString"}
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "_1", "mapping": '$."_1"', "type": "DfString"}
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "_1", "mapping": '$."_1"', "type": "DfString"}
                    ]
                },
            ],
            "statuses": [
                {"unsupported_columns": [], "error_message": None},
                {"unsupported_columns": [], "error_message": None},
                {"unsupported_columns": [], "error_message": None}
            ],
            "global_status": {
                "error_message": None
            }
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {"_1": "These are some words"},
            {"_1": "Missing a word"},
            {"_1": "But this has all"}
        ]
    },
    {
        'name': 'CSV - Test Load Num Rows Limit',
        'data': (
            "A,B,C\n"
            "1,bee,see\n"
            "2,be,sea\n"
            "3,b,c\n"
        ).encode('utf-8'),
        'input_serial': {
            "CSV": {
                "FileHeaderInfo": "USE",
                "QuoteEscapeCharacter": '"',
                "RecordDelimiter": "\n",
                "FieldDelimiter": ",",
                "QuoteCharacter": '"',
                "AllowQuotedRecordDelimiter": True}
        },
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"A":"1","B":"bee","C":"see"}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfString"},
                        {"name": "C", "mapping": '$."C"', "type": "DfString"},
                    ]
                },
            ],
            "statuses": [
                {"unsupported_columns": [], "error_message": None},
            ],
            "global_status": {
                "error_message": None
            }
        },
        'load_num_rows': 2,
        'expected_data_rows': [
            {"A": 1, "B": "bee", "C": "see"},
            {"A": 2, "B": "be", "C": "sea"},
        ]
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", preview_load_tests, ids=lambda case: case["name"])
def test_preview_response_and_load(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
    check_test_case(target_name, temp_path, test_case)


# These are to tests are two fold:
#  1. Do we properly rename the column as is expected
#  2. Are we able to properly escape the source column name in the s3 query
special_chars_in_column_names = [
    {
        'name': 'CSV with double quote in column name',   # noqa: W605
        'data': (
            'some"word,other,thing\n'
            '1,2,3\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"some\\"word":"1","other":"2","thing":"3"}\n',
            ],
            "schemas": [{
                "rowpath": "$",
                "columns": [
                    {"name": 'SOME_WORD', "mapping": '$."some""word"', "type": "DfInt64"},
                    {"name": 'OTHER', "mapping": '$."other"', "type": "DfInt64"},
                    {"name": 'THING', "mapping": '$."thing"', "type": "DfInt64"},
                ]
            }],
            "statuses": [
                {"unsupported_columns": [], "error_message": None},
            ],
            "global_status": {
                "error_message": None
            }
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {'SOME_WORD': 1, 'OTHER': 2, 'THING': 3}
        ]
    },
    {
        'name': 'JSONL with many double quotes in column name',
        'data': (
            json.dumps({'some"other"words""': 1, "other": 2, "thing": 3}) + '\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 1,
        'expected_preview_response': {
            "rows": [
                '{"some\\"other\\"words\\"\\"":1,"other":2,"thing":3}\n',
            ],
            "schemas": [{
                "rowpath": "$",
                "columns": [
                    {"name": 'SOME_OTHER_WORDS__', "mapping": '$."some""other""words"""""', "type": "DfInt64"},
                    {"name": 'OTHER', "mapping": '$."other"', "type": "DfInt64"},
                    {"name": 'THING', "mapping": '$."thing"', "type": "DfInt64"},
                ]
            }],
            "statuses": [
                {"unsupported_columns": [], "error_message": None},
            ],
            "global_status": {
                "error_message": None
            }
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {'SOME_OTHER_WORDS__': 1, 'OTHER': 2, 'THING': 3}
        ]
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", special_chars_in_column_names,
                         ids=lambda case: case["name"])
def test_special_chars_in_column_names(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
    check_test_case(target_name, temp_path, test_case)


json_middle_column_test = [
    {
        'name': 'JSON with missing middle column',   # noqa: W605
        'data': b'\n'.join([
            json.dumps({"A": 1, "B": 2, "C": 3}).encode('utf-8'),
            json.dumps({"A": 4, "C": 6}).encode('utf-8'),
            json.dumps({"A": 7, "B": 8, "C": 9}).encode('utf-8'),
        ]) + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'preview_num_rows': 3,
        'expected_preview_response': {
            "rows": [
                '{"A":1,"B":2,"C":3}\n',
                '{"A":4,"C":6}\n',
                '{"A":7,"B":8,"C":9}\n',
            ],
            "schemas": [
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
                    ]
                },
                {
                    "rowpath": "$",
                    "columns": [
                        {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                        {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                        {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
                    ]
                }
            ],
            "statuses": [
                {"error_message": None, "unsupported_columns": []},
                {"error_message": None, "unsupported_columns": []},
                {"error_message": None, "unsupported_columns": []},
            ],
            "global_status": {"error_message": None}
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {"A": 1, "B": 2, "C": 3},
            {"A": 4, "C": 6},
            # Previous to this change, AWS put 6 in B.
            # AWS is really bad at parsing ...
            {"A": 7, "B": 8, "C": 9},
        ]
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", json_middle_column_test,
                         ids=lambda case: case["name"])
def test_missing_middle_column(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
    check_test_case(target_name, temp_path, test_case)


##
# Test load with Specified Schema
###################################
#
# These tests will be when we load data and specify the schema manually.
# This is so that we can see what happens when different casting options
# are used on different data types.
#
# In particular, I need to test what happens when we cast Dict and List
# to a some other field. It should fail with a message in the error table.
#

specified_schema_tests = [
    {
        'name': 'JSON with cast Array column as Int',   # noqa: W605
        'data': b'\n'.join([
            json.dumps({"A": 1, "B": 2, "C": 3}).encode('utf-8'),
            json.dumps({"A": 4, "B": 5, "C": 6}).encode('utf-8'),
            json.dumps({"A": 7, "B": 8, "C": ["foo"]}).encode('utf-8'),
        ]) + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'schema': {
            "rowpath": "$",
            "columns": [
                {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
            ]
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {"A": 1, "B": 2, "C": 3},
            {"A": 4, "B": 5, "C": 6},
        ],
        'expected_comp_rows': [
            {'XCALAR_ICV': 'ERROR: Found Array in column(C), expected(DfInt64)'}
        ]
    },
    {
        'name': 'JSON with cast Object column as Int',
        'data': b'\n'.join([
            json.dumps({"A": 1, "B": 2, "C": 3}).encode('utf-8'),
            json.dumps({"A": 4, "B": 5, "C": 6}).encode('utf-8'),
            json.dumps({"A": 7, "B": 8, "C": {"foo": "bar"}}).encode('utf-8'),
        ]) + b'\n',
        'input_serial': INPUT_SERIAL_JSONL,
        'schema': {
            "rowpath": "$",
            "columns": [
                {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
            ]
        },
        'load_num_rows': 3,
        'expected_data_rows': [
            {"A": 1, "B": 2, "C": 3},
            {"A": 4, "B": 5, "C": 6},
        ],
        'expected_comp_rows': [
            {'XCALAR_ICV': 'ERROR: Found Object in column(C), expected(DfInt64)'}
        ]
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", specified_schema_tests,
                         ids=lambda case: case["name"])
def test_jsonl_specified_schema(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
    check_test_case_specified_schema(target_name, temp_path, test_case)


specified_schema_multifile_tests = [
    {
        'name': 'JSON with cast Array column as Int',   # noqa: W605
        'data': json.dumps({"A": 1, "B": 2, "C": 3}).encode('utf-8') + b'\n',
        # This isn't super clear that we are creating one file in each of
        # these "directories"
        'path_parts': [
            'dir/',
            'dir/sub1/',
            'dir/sub2/'
        ],
        'input_serial': INPUT_SERIAL_JSONL,
        'schema': {
            "rowpath": "$",
            "columns": [
                {"name": "A", "mapping": '$."A"', "type": "DfInt64"},
                {"name": "B", "mapping": '$."B"', "type": "DfInt64"},
                {"name": "C", "mapping": '$."C"', "type": "DfInt64"},
            ]
        },
        'expected_data_rows': [  # will have one file per path_part
            {"A": 1, "B": 2, "C": 3},
            {"A": 1, "B": 2, "C": 3},
            {"A": 1, "B": 2, "C": 3},
        ],
        'expected_comp_rows': []
    },
]


@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
@pytest.mark.parametrize("test_case", specified_schema_multifile_tests,
                         ids=lambda case: case["name"])
def test_jsonl_specified_schema_multifile(target_name, temp_path, test_case):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
    check_test_case_specified_schema(target_name, temp_path, test_case)


##
# Manual Tests
############################


@pytest.mark.skip("ttucker's load many files - BROKEN")
@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
def test_load_many_files(target_name, temp_path):
    with setup_load_wizard() as (client, session, workbook):

        load_id = get_load_id(client, session)

        num_preview_rows = 1
        num_load_rows = None  # A value of None loads all files
        path = '/xcfield/ttucker/integer_files/'
        INPUT_SERIAL_CSV = {
            'CSV': {
                'FileHeaderInfo': 'USE',
                'FieldDelimiter': ',',
                'RecordDelimiter': '\n',
                'AllowQuotedRecordDelimiter': True,
            }
        }
        input_serial_json = json.dumps(INPUT_SERIAL_CSV)
        # input_serial_json = json.dumps(INPUT_SERIAL_JSONL)

        source_args = [{
            "targetName": target_name,
            "path": path,
            "fileNamePattern": "*.csv",
            "recursive": False
        }]
        source_args_json = json.dumps(source_args)

        source_arg = source_args[0]
        target = client.get_data_target(source_arg["targetName"])
        resp = target.list_files(source_arg["path"],
                                 source_arg["fileNamePattern"],
                                 source_arg["recursive"])

        # [{'isDir': False, 'size': 220130, 'mtime': 1598035760, 'name': 'int_20_col_1000_rows_0.csv'},...]
        non_error_files = [ff['name'] for ff in resp.files
                           if 'error' not in ff['name']
                           and not ff['isDir']]

        error_files = [ff['name'] for ff in resp.files
                       if 'error' in ff['name']]
        file_path = os.path.join(path, non_error_files[0])

        ret = call_preview_source(
            client, session, target_name, file_path, input_serial_json, num_preview_rows, load_id)
        schema = ret["schemas"][0]
        schema_json = json.dumps(schema)

        table_name = 'TEST_TABLE_FOO'

        # Need tables CTX .. do later.
        tables_ctx = do_load(
            client, session, source_args_json, input_serial_json,
            schema_json, load_id, num_rows=20)

        with contextlib.ExitStack() as stack:
            tables = {nn: stack.enter_context(cc)
                      for nn, cc in tables_ctx.items()}
            # for rec in tables["load"].records():
            #     print(f"{rec}")


@pytest.mark.skip("ttucker's one off test - BROKEN")
@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
def test_load_one_file(target_name, temp_path):
    with setup_load_wizard() as (client, session, workbook):

        load_id = get_load_id(client, session)

        num_rows = 20
        file_path = '/saas-load-test/Real_World_Test/All_Lending_Club_loan_accepted_2007_to_2018Q4.csv'
        file_path = '/sharedinf-samplebucket-876030232190-us-west-2/tpch-tables/dates.csv'
        file_path = '/saas-load-test/Real_World_Test/Tweets_during_RM_vs_Liverpool_data.json'
        file_path = '/xcfield/saas-load-test/Error_Handling_Test/2500_Cols/2500_cols0.0.parquet'
        INPUT_SERIAL_CSV = {
            'CSV': {
                'FileHeaderInfo': 'USE',
                'FieldDelimiter': ',',
                'RecordDelimiter': '\n',
                'AllowQuotedRecordDelimiter': True,
            }
        }
        input_serial_json = json.dumps(INPUT_SERIAL_CSV)
        input_serial_json = json.dumps(INPUT_SERIAL_PARQUET)

        source_args = [{
            "targetName": target_name,
            "path": file_path,
            "fileNamePattern": "*.csv",
            "recursive": False
        }]
        source_args_json = json.dumps(source_args)

        ret = call_preview_source(
            client, session, target_name, file_path,
            input_serial_json, num_rows, load_id)
        schema = ret["schemas"][0]
        schema_json = json.dumps(schema)

        table_name = 'TEST_TABLE_FOO'

        # Need tables CTX .. do later.
        tables_ctx = do_load(
            client, session, source_args_json, input_serial_json,
            schema_json, load_id, num_rows=20)

        with contextlib.ExitStack() as stack:
            tables = {nn: stack.enter_context(cc)
                      for nn, cc in tables_ctx.items()}
            # for rec in tables["load"].records():
            #     print(f"{rec}")


#
# Data Generators for Ramesh Tests
#


def guass_rows_gen(n):
    # Generate a large number of rows
    # Validate with Guass formula: ((END - START + 1)/2) * (END + START)
    for ii in range(n):
        yield (("ONE", 1 + ii), ("TWO", 2 + ii), ("THREE", 3 + ii))


def csv_single_file_gen(tup_gen):
    rec_delim = '\n'
    field_delim = ','
    header = None
    for rec_tup in tup_gen:
        if header is None:
            header = [f'{field_tup[0]}' for field_tup in rec_tup]
            header = f'{field_delim.join(header)}{rec_delim}'
            yield header.encode('utf-8')
        row = [f'{field_tup[1]}' for field_tup in rec_tup]
        row = f'{field_delim.join(row)}{rec_delim}'
        yield row.encode('utf-8')


def csv_mutliple_file_gen(tup_gen):
    rec_delim = '\n'
    field_delim = ','
    for rec_tup in tup_gen:
        header = [f'{field_tup[0]}' for field_tup in rec_tup]
        header = f'{field_delim.join(header)}{rec_delim}'
        row = [f'{field_tup[1]}' for field_tup in rec_tup]
        row = f'{field_delim.join(row)}{rec_delim}'
        yield (header + row).encode('utf-8')


def jsonl_file_gen(tup_gen):
    rec_delim = '\n'
    for rec_tup in tup_gen:
        row = json.dumps({field_tup[0]: field_tup[1] for field_tup in rec_tup})
        row = f'{row}{rec_delim}'
        yield row.encode('utf-8')


def parquet_single_file_gen(tup_gen):
    # XXX NOTE: this function works, but our installed versions of parquet
    # and pandas is crap. For now, gen files outside of Pytest.
    tups = next(tup_gen)
    columns = [tup[0] for tup in tups]
    first_row = (tup[1] for tup in tups)

    def inner_gen(tup_gen):
        first_run = True
        if first_run:
            first_run = False
            yield first_row
        for tups in tup_gen:
            yield (tup[1] for tup in tups)

    data_df = pd.DataFrame(inner_gen(tup_gen), columns=columns)
    buffer = BytesIO()

    table = pa.Table.from_pandas(data_df)
    pq.write_table(table, buffer)
    # data_df.to_parquet(buffer)
    return buffer.getvalue()


def parquet_multi_file_gen(tup_gen):
    for tups in tup_gen:
        columns = [tup[0] for tup in tups]
        row = (tup[1] for tup in tups)

        def inner_gen():
            yield row

        data_df = pd.DataFrame(inner_gen(), columns=columns)
        buffer = BytesIO()
        print(f"data_df: {data_df}")
        table = pa.Table.from_pandas(data_df)
        print(f"table: {table}")
        pq.write_table(table, buffer)
        # data_df.to_parquet(buffer)
        yield buffer.getvalue()


def multiple_seq_files(target_name, base_path, file_gen, num_files):
    """ Ensures that we have at least N number of sequential files """
    if num_files > 10_000:
        raise ValueError("num_files must not exceed 10,000")
    target_data = get_target_data(target_name)
    target = build_target(target_name, target_data, base_path)

    files = target.get_files(base_path, '*', False)

    # Check if files exist
    if len(files) >= num_files:
        print(f"Found {num_files} files at: {base_path}")
    else:
        for ii, b in enumerate(file_gen(guass_rows_gen(num_files))):
            file_path = os.path.join(base_path, f"file_{ii:05}.ext")
            target.open(file_path, 'wb').write(b)
            print(f"Wrote file: {file_path}, {b}")


#
# Ramesh Asked for these tests:
# For JSONL, Parquet and CSV:
#  - test 1 million rows and validate data.
#  - test 1000 files and validate data.
#  - test loading 100 tables of 100 records and validate data.
#
@pytest.mark.parametrize("file_name,data_gen,input_serial", [
    ("1000000_rows/file.parquet", parquet_single_file_gen, INPUT_SERIAL_PARQUET),
    ("1000000_rows/file.jsonl", jsonl_file_gen, INPUT_SERIAL_JSONL),
    ("1000000_rows/file.csv", csv_single_file_gen, INPUT_SERIAL_CSV)
])
@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
def test_one_million_rows(target_name, temp_path, file_name, data_gen, input_serial):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    target_data = get_target_data(target_name)
    target = build_target(target_name, target_data, temp_path)

    num_rows = 1_000_000
    path = os.path.join(temp_path, file_name)

    # Check if file exists
    files = target.get_files(path, '*', False)
    if len(files) == 1:
        print(f"Found file at: {path}")
    # Create file..
    else:
        data_row_gen = data_gen(guass_rows_gen(num_rows))
        data = b''.join([row for row in data_row_gen])
        with target.open(path, 'wb') as file:
            file.write(data)
            print(f"Created file at: {path}")

    # Make load parameters
    source_args = [{
        "targetName": target_name,
        "path": path,
        "fileNamePattern": file_name.split('/')[-1],
        "recursive": False
    }]
    check_guass_consecutive_numbers(num_rows, input_serial, source_args)


@pytest.mark.parametrize("data_path,file_gen,input_serial", [
    ("1000_csv_files/", csv_mutliple_file_gen, INPUT_SERIAL_CSV),
    ("1000_jsonl_files/", jsonl_file_gen, INPUT_SERIAL_JSONL),
    ("1000_parquet_files/", parquet_multi_file_gen, INPUT_SERIAL_PARQUET),
])
@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
def test_1k_file_load(target_name, temp_path, file_gen, data_path, input_serial):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    # Make sure files are available
    num_files = 1000
    path = os.path.join(temp_path, data_path)
    multiple_seq_files(target_name, path, file_gen, num_files)

    # Make load parameters
    source_args = [{
        "targetName": target_name,
        "path": path,
        "fileNamePattern": "*",
        "recursive": False
    }]
    check_guass_consecutive_numbers(num_files, input_serial, source_args)


@pytest.mark.parametrize("data_path,file_gen,input_serial", [
    ("1000_csv_files/", csv_mutliple_file_gen, INPUT_SERIAL_CSV),
    ("1000_jsonl_files/", jsonl_file_gen, INPUT_SERIAL_JSONL),
    ("1000_parquet_files/", parquet_multi_file_gen, INPUT_SERIAL_PARQUET),
])
@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
def test_100_tables(target_name, temp_path, file_gen, data_path, input_serial):
    if not cloud_tests_enabled():
        pytest.skip(CLOUD_TESTS_DISABLED_MSG)

    num_files = 100
    path = os.path.join(temp_path, data_path)
    multiple_seq_files(target_name, path, file_gen, num_files)

    schema = {
        "rowpath": "$",
        "columns": [
            {"name": "ONE", "mapping": "$.ONE", "type": "DfInt64"},
            {"name": "TWO", "mapping": "$.TWO", "type": "DfInt64"},
            {"name": "THREE", "mapping": "$.THREE", "type": "DfInt64"},
        ]
    }

    # Make load parameters
    source_args = [{
        "targetName": target_name,
        "path": path,
        "fileNamePattern": "re:file_000[0-9][0-9].ext",
        "recursive": False
    }]
    source_args_json = json.dumps(source_args)
    input_serial_json = json.dumps(input_serial)
    schema_json = json.dumps(schema)

    num_tables = 100
    num_count = num_files

    with setup_load_wizard() as (client, session, workbook):
        load_id = get_load_id(client, session)
        load_ctxs = []
        for ii in range(num_tables):
            print(f"Loading table num {ii}")
            load_ctxs.append(do_load(client, session, source_args_json,
                                     input_serial_json, schema_json, load_id))

        print(f"Starting load of {num_tables} tables")
        with contextlib.ExitStack() as stack:
            load_tables = [stack.enter_context(load_ctx) for load_ctx in load_ctxs]
            print(f"Loaded all {num_tables} tables")

            for ii, load_table in enumerate(load_tables):
                one_sum, two_sum, three_sum = 0, 0, 0
                for rec in load_table.records():
                    one_sum = one_sum + rec['ONE']
                    two_sum = two_sum + rec['TWO']
                    three_sum = three_sum + rec['THREE']
                exp_one_sum = (num_count / 2) * (num_count + 1)
                exp_two_sum = (num_count / 2) * (num_count + 3)
                exp_three_sum = (num_count / 2) * (num_count + 5)

                comp_list = [("ONE", one_sum, exp_one_sum),
                             ("TWO", two_sum, exp_two_sum),
                             ("THREE", three_sum, exp_three_sum)]

                for tup in comp_list:
                    if tup[1] != tup[2]:
                        raise ValueError(f"{tup[0]} mismatch: {tup[1]} != {tup[2]}")
                    else:
                        print(f"Validated table({ii}) {tup[0]} data is correct")


def check_guass_consecutive_numbers(num_count, input_serial, source_args):
    schema = {
        "rowpath": "$",
        "columns": [
            {"name": "ONE", "mapping": "$.ONE", "type": "DfInt64"},
            {"name": "TWO", "mapping": "$.TWO", "type": "DfInt64"},
            {"name": "THREE", "mapping": "$.THREE", "type": "DfInt64"},
        ]
    }
    source_args_json = json.dumps(source_args)
    input_serial_json = json.dumps(input_serial)
    schema_json = json.dumps(schema)

    # Do the load...
    with setup_load_wizard() as (client, session, workbook):
        load_id = get_load_id(client, session)
        load_table_ctx = do_load(client, session, source_args_json,
                                 input_serial_json, schema_json, load_id)

        one_sum, two_sum, three_sum = 0, 0, 0
        with load_table_ctx as load_table:
            for rec in load_table.records():
                one_sum = one_sum + rec['ONE']
                two_sum = two_sum + rec['TWO']
                three_sum = three_sum + rec['THREE']

        # Formula: (end_num - start_num + 1) * (end_num + start_num)
        # non-reduced: exp_one_sum = ((num_rows + 0 - 1 + 1) // 2) * (num_rows + 1 + 0)
        exp_one_sum = (num_count / 2) * (num_count + 1)
        exp_two_sum = (num_count / 2) * (num_count + 3)
        exp_three_sum = (num_count / 2) * (num_count + 5)

        comp_list = [("ONE", one_sum, exp_one_sum),
                     ("TWO", two_sum, exp_two_sum),
                     ("THREE", three_sum, exp_three_sum)]

        print(f"Validating all data was present ... with math")
        for tup in comp_list:
            if tup[1] != tup[2]:
                raise ValueError(f"{tup[0]} mismatch: {tup[1]} != {tup[2]}")
            else:
                print(f"MATCH!! {tup[0]}: {tup[1]} == {tup[2]}")

#
#
#
#
#
#   Old stuff after this...
#
#
#
#
#
#

#
#
#
#
#
#   Ignore everything after this......
#
#
#
#
#
#

#
#
#
#
#
#   Old stuff after this...
#
#
#
#
#
#


##
# File Formats
###############################################################################


all_file_formats = [
    {"type": "CSV", "suffix": ".csv", "input_serial": INPUT_SERIAL_CSV},
    {"type": "CSV_GZ", "suffix": ".csv.gz", "input_serial": INPUT_SERIAL_CSV_GZ},
    {"type": "CSV_BZ2", "suffix": ".csv.bz2", "input_serial": INPUT_SERIAL_CSV_BZ2},
    {"type": "JSONL", "suffix": ".json", "input_serial": INPUT_SERIAL_JSONL},
    {"type": "JSONL_GZ", "suffix": ".json.gz", "input_serial": INPUT_SERIAL_JSONL_GZ},
    {"type": "JSONL_BZ2", "suffix": ".json.bz2", "input_serial": INPUT_SERIAL_JSONL_BZ2},
    {"type": "JSON_ARRAY", "suffix": ".json", "input_serial": INPUT_SERIAL_JSONDOC},
]
cloud_file_formats = all_file_formats
local_file_formats = [ff for ff in all_file_formats if ff['type'] != 'JSON_ARRAY']

##
# File Type Builders
###############################################################################


basic_data = {
    "name":
        "BASIC_DATA_ONE",
    "schema": [
        {"name": "A", "mapping": "$.A", "type": "DfInt64"},
        {"name": "B", "mapping": "$.B", "type": "DfString"},
        {"name": "C", "mapping": "$.C", "type": "DfFloat64"},
        {"name": "IS_ICV", "mapping": "$.IS_ICV", "type": "DfString"},
    ],
    "records": [
        {"A": 1, "B": "one", "C": 0.1, "IS_ICV": "false"},
        {"A": 2, "B": "two", "C": 0.2, "IS_ICV": "false"},
        {"A": 3, "B": "three", "C": 0.3, "IS_ICV": "false"},
        {"A": 4, "B": "four", "C": 0.4, "IS_ICV": "false"}
    ]
}


def get_file_from_data_desc(data_desc, file_type):
    # this is basic for right now, need to specify delimiers in future.
    field_delim = ','
    record_delim = '\n'
    if file_type == "CSV":
        header_line = field_delim.join(
            [f"{s['name']}" for s in data_desc["schema"]]) + record_delim
        record_data = ''.join([
            f"{field_delim.join([str(v) for v in rec.values()])}{record_delim}"
            for rec in data_desc['records']
        ])
        return f"{header_line}{record_data}".encode('utf-8')
    elif "GZ" in file_type:
        return gzip.compress(get_file_from_data_desc(data_desc, file_type[:-3]))
    elif "BZ2" in file_type:
        return bz2.compress(get_file_from_data_desc(data_desc, file_type[:-4]))
    elif file_type == "JSONL":
        record_data = ''.join([
            f"{json.dumps(rec)}{record_delim}" for rec in data_desc['records']
        ])
        return record_data.encode('utf-8')
    elif file_type == "JSON_ARRAY":
        return json.dumps(data_desc['records']).encode('utf-8')
    else:
        raise ValueError(f"Unknown file_type({file_type} specified")
##
# Functionality Data
###############################################################################
# These cases illustrate the exact behavior of AWS

# XXX
# XXX
# XXX

# Tests to write/fix
# - Internal column names.
# - Validate that Arrays fail schema discover.
# - Validate that schema size fails discover.

# XXX
# XXX
# XXX


functionality_cases = [    # noqa: W605
    # {
    #     'name': 'Internal Column Names',    # noqa: W605
    #     'data': (json.dumps(
    #         {cc: 'abc' for cc in COMPLIMENTS_COLUMN_NAMES}) + '\n'
    #     ).encode('utf-8'),
    #     'input_serial': INPUT_SERIAL_JSONL,
    #     'expected_rows': [{
    #         f'COL_{cc}': 'abc' for cc in COMPLIMENTS_COLUMN_NAMES
    #     }]
    # },
    {
        'name': 'Simple JSON: Two Ints',   # noqa: W605
        'data': ('{"a": 1, "b": 2}\n'
                 '{"a": 2, "b": 2}\n'
                 '{"a": 3, "b": 2}\n'
                 '{"a": 4, "b": 2}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': 1, 'B': 2},
            {'A': 2, 'B': 2},
            {'A': 3, 'B': 2},
            {'A': 4, 'B': 2}
        ]    # NOTE: column names are CAPITALIZED
    },
    {
        'name': 'Simple JSON: Null and Boolean',
        'data': ('{"a": null, "b": true, "c": false}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [{'B': True, 'C': False}]
        # NOTE: AWS does not acknowledge the null existing.
    },
    {
        'name': 'Simple JSON: Three Strings',
        'data': ('{"a": "What", "b": "was", "c": "that"}\n'
                 '{"a": "thing", "b": "I", "c": "thought"}\n'
                 '{"a": "I", "b": "would", "c": "think?"}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': 'What', 'B': 'was', 'C': 'that'},
            {'A': 'thing', 'B': 'I', 'C': 'thought'},
            {'A': 'I', 'B': 'would', 'C': 'think?'}
        ]
    },
    {
        'name': 'Simple JSON: Four Floats',
        'data': (
            '{"e": 2.718, "pi": 3.141, "phi": 1.618, "sqrt2": 1.414}\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'E': 2.718, 'PI': 3.141, 'PHI': 1.618, 'SQRT2': 1.414}
        ]
    },
    {
        'name': 'Complex JSON: Nested Objects of Ints',
        'data': ('{"a": 1, "b": {"c": 2, "d": 3}}\n'
                 '{"a": 4, "b": {"c": 5, "d": 6}}\n'
                 '{"a": 7, "b": {"c": 8, "d": 9}}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': 1, 'C': 2, 'D': 3},
            {'A': 4, 'C': 5, 'D': 6},
            {'A': 7, 'C': 8, 'D': 9}
        ]
    },
    {
        'name': 'Complex JSON: Deep nested Objects (11 deep to be exact)',
        'data': (
            '{"A": {"B": {"C": {"D": {"E": {"F": {"G": {"H": {"I": {"J": {"K": "these"}}}}}}}}}}}\n'
            '{"A": {"B": {"C": {"D": {"E": {"F": {"G": {"H": {"I": {"J": {"K": "are"}}}}}}}}}}}\n'
            '{"A": {"B": {"C": {"D": {"E": {"F": {"G": {"H": {"I": {"J": {"K": "strings"}}}}}}}}}}}\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'K': "these"},
            {'K': "are"},
            {'K': "strings"}
        ]
    },
    {
        'name': 'Complex JSON: Nested Objects with same Key Name',
        'data': ('{"asdf": {"B": "Up"}, "fdsa": {"B": "Down"}}\n'
                 '{"asdf": {"B": "Yin"}, "fdsa": {"B": "Yang"}}\n'
                 '{"asdf": {"B": "Black"}, "fdsa": {"B": "White"}}\n'
                 ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {"B": "Up", "B0": "Down"},
            {"B": "Yin", "B0": "Yang"},
            {"B": "Black", "B0": "White"}
        ]    # NOTE: we rename the second column
    },
    {    # CSV CASES
        'name': 'Simple CSV: Header line with No quotes',
        'data': ('foo,bar,goo,car\n'
                 '1,2,3,4\n'
                 '5,6,7,8\n'
                 '9,0,1,2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_rows': [
            {'FOO': 1, 'BAR': 2, 'GOO': 3, 'CAR': 4},
            {'FOO': 5, 'BAR': 6, 'GOO': 7, 'CAR': 8},
            {'FOO': 9, 'BAR': 0, 'GOO': 1, 'CAR': 2},
        ]
    },
    {
        'name': 'Simple CSV: Header line with minimal quotes',
        'data': ('foo poo,bar,goo,car\n'
                 '1,"2",3,4\n'
                 '5,"6",7,8\n'
                 '9,"0",1,2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_rows': [
            {'COL_FOOPOO': 1, 'BAR': 2, 'GOO': 3, 'CAR': 4},
            {'COL_FOOPOO': 5, 'BAR': 6, 'GOO': 7, 'CAR': 8},
            {'COL_FOOPOO': 9, 'BAR': 0, 'GOO': 1, 'CAR': 2},
        ]    # apparently we need to support spaces
    },
    {
        'name': 'Simple CSV: ignore header line',
        'data': ('foo,bar,goo,car\n'
                 '1,2,3,4\n'
                 '5,6,7,8\n'
                 '9,0,1,2\n').encode('utf-8'),
        'input_serial': {'CSV': {'FileHeaderInfo': 'IGNORE'}},
        'expected_rows': [
            {'_1': 1, '_2': 2, '_3': 3, '_4': 4},
            {'_1': 5, '_2': 6, '_3': 7, '_4': 8},
            {'_1': 9, '_2': 0, '_3': 1, '_4': 2}]
    },
    {
        'name': 'Simple CSV: No header line',
        'data': ('1,2,3,4\n'
                 '5,6,7,8\n'
                 '9,0,1,2\n').encode('utf-8'),
        'input_serial': {'CSV': {'FileHeaderInfo': 'NONE'}},
        'expected_rows': [
            {'_1': 1, '_2': 2, '_3': 3, '_4': 4},
            {'_1': 5, '_2': 6, '_3': 7, '_4': 8},
            {'_1': 9, '_2': 0, '_3': 1, '_4': 2}]
    },
    {
        'name': 'Complex CSV: Default Options: Escape Character (\) inside quotes',
        'data': (
            'foo,bar,goo,car\n'
            '1,"te\st",3,4\n'
            '5,"te\\st",7,8\n'    # this is the same as the line above
            '9,"test",1,2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_rows': [
            {'FOO': 1, 'BAR': 'te\\st', 'GOO': 3, 'CAR': 4},
            {'FOO': 5, 'BAR': 'te\\st', 'GOO': 7, 'CAR': 8},
            {'FOO': 9, 'BAR': 'test', 'GOO': 1, 'CAR': 2},
        ]    # NOTE: I inverted the use of \ and \\ to show they are the same
    },
    {
        'name': 'Complex CSV: Default Options: Field Delim (,) inside quotes WORKS',
        'data': ('foo,bar,goo,car\n'
                 '1,"te,st",3,4\n'
                 '9,"test",1,2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_rows': [
            {'FOO': 1, 'BAR': 'te,st', 'GOO': 3, 'CAR': 4},
            {'FOO': 9, 'BAR': 'test', 'GOO': 1, 'CAR': 2},
        ]    # NOTE: Apparently AllowQuotedRecordDelimiter defaults to True
    },
    {
        'name': 'Complex CSV: Long Field Name Truncated (COL_<first 100>)',
        'data': (
            f'{"t" * 256}\n'    # One character over XcalarApiMaxFieldNameLen
            '1\n'
            '2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_rows': [
            {f'COL_{"T" * 100}': 1},
            {f'COL_{"T" * 100}': 2},
        ]    # NOTE: This is good because this is less than our max field name len of 255
    },
    {
        'name': 'Complex CSV: Long Field Name trunc. does not cause dups',
        'data': (f'{"t" * 100}A,{"t" * 100}B\n'
                 '1,9\n'
                 '2,8\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_rows': [
            {f'COL_{"T" * 100}_1': 1, f'COL_{"T" * 100}_2': 9},
            {f'COL_{"T" * 100}_1': 2, f'COL_{"T" * 100}_2': 8},
        ]    # NOTE: This is good because it keeps these as separate fields
    },
    {
        'name': 'JSONL: AWS Protected Key Word Column Name (level)',
        'data': ('{"level": 123}\n'
                 '{"level": 456}\n'
                 '{"level": 789}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {"LEVEL": 123},
            {"LEVEL": 456},
            {"LEVEL": 789}
        ]    # ENG-9045 - Column name of "level" causes issue
    }
]

# We need a bunch of tests which confirm that we can parse
# different format records which contain the CSV record, field,
# and escape characters we are using in the output parser.
formatting_tests = [
    {
        'name': 'Formatting: JSONL with field separator(,) in field',   # noqa: W605
        'data': (
            '{"a": "a1,a1", "b": 1}\n'
            '{"a": "a2,a2", "b": 2}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': "a1,a1", 'B': 1},
            {'A': "a2,a2", 'B': 2},
        ]
    },
    {
        'name': 'Formatting: JSONL with record separator(\n) in field',
        'data': (
            '{"a": "a1\na1", "b": 1}\n'
            '{"a": "a2\na2", "b": 2}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': 'a1\na1', 'B': 1},
            {'A': 'a2\na2', 'B': 2},
        ]  # Think about this one. I don't think there is a way around this?
    },
    {
        'name': 'Formatting: JSONL with quote character(") in field',
        'data': (
            # We need double escapes so that python writes
            # this string as `"a1\"a1"` in the JSON
            '{"a": "a1\\"a1", "b": 1}\n'
            '{"a": "a2\\"a2", "b": 2}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': 'a1"a1', 'B': 1},
            {'A': 'a2"a2', 'B': 2},
        ]
    },
    {
        'name': 'Formatting: JSONL with escape character(\) in field',
        'data': (
            # We need double escapes so that python writes
            # this string as `"a1\\a1"` in the JSON
            '{"a": "a1\\\\a1", "b": 1}\n'
            '{"a": "a2\\\\a2", "b": 2}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': 'a1\\a1', 'B': 1},
            {'A': 'a2\\a2', 'B': 2},
        ]
    },
    {
        'name': 'Formatting: JSONL with spaces in key name',
        'data': (
            '{"a a": 1, "b b": 1}\n'
            '{"a a": 2, "b b": 2}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'COL_AA': 1, 'COL_BB': 1},
            {'COL_AA': 2, 'COL_BB': 2},
        ]
    },
    {
        'name': 'Formatting: JSONL (nested) with spaces in key name',
        'data': (
            '{"a a": {"B   B": {"C    C C": 1}} ,  "X X": 2}\n'
            '{"a a": {"B   B": {"C    C C": 3}} ,  "X X": 4}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'COL_CCC': 1, 'COL_XX': 2},
            {'COL_CCC': 3, 'COL_XX': 4},
        ]
    },
]


# XXX
# XXX Do something with these tests ...
# XXX
will_fail = [
    {
        'name':
            '[22] Complex CSV: Default Options: Large field (32KB + 1)',
        'data': ('bigfield\n'
                 f'{"T" * (2 ** 10 * 32 + 1)}\n').encode('utf-8'),
        'input_serial': {
            'CSV': {
                'FileHeaderInfo': 'USE'
            }
        },
        'expected_rows': [{
            'BIGFIELD': f'{"T" * (2 ** 15 + 1)}'
        }]    # NOTE: Hrmm... I expected this to fail, but it didn't?
        # XXX Found out from Brent that Datasets don't have field size limits.
    },
    # This will cause childnode assert fail. I don't know how to test this?
    {
        'name':
            '[24] Complex CSV: Large Record (>128K)',
        'data': ('first,second,third,forth,plusone\n'
                 f'{"T" * (2 ** 10 * 32)},'
                 f'{"T" * (2 ** 10 * 32)},'
                 f'{"T" * (2 ** 10 * 32)},'
                 f'{"T" * (2 ** 10 * 32)},'
                 'T').encode('utf-8'),
        'input_serial': {
            'CSV': {
                'FileHeaderInfo': 'USE'
            }
        },
        'expected_rows': [{
            f'COL_{"T" * 100}_1': '1',
            f'COL_{"T" * 100}_2': '9'
        }, {
            f'COL_{"T" * 100}_1': '2',
            f'COL_{"T" * 100}_2': '8'
        }]
    },    # record: 0, field '_X_SOURCE_DATA': size(131138) larger than page size(131072)
    {
        'name': '[4] Complex JSON: A list of lists',
        'data': ('{"A": [[1, 2, 3], [4, 5, 6]]}\n'
                 '{"A": [[6, 7, 8], [9, 0, 1]]}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': '[1, 2, 3, 4, 5, 6]'},
            {'A': '[6, 7, 8, 9, 0, 1]'}
        ]    # Lists are concatenated. JSON Path: "$.A[0:][0:]"
    },
    {
        'name': '[7] Complex JSON: Nested Lists of Str, Int, Float in Objects',
        'data': (
            '{"A": ["foo", "bar"], "w00t": {"B": [3.141, 1.618], "v00t": {"C": [69, 69]}}}\n'
            '{"A": ["goo", "car"], "w00t": {"B": [9.865, 2.617], "v00t": {"C": [69, 69]}}}\n'
            '{"A": ["hoo", "dar"], "w00t": {"B": [30.988, 4.235], "v00t": {"C": [69, 69]}}}\n'
        ).encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'A': "['foo', 'bar']", 'B': '[3.141, 1.618]', 'C': '[69, 69]'},
            {'A': "['goo', 'car']", 'B': '[9.865, 2.617]', 'C': '[69, 69]'},
            {'A': "['hoo', 'dar']", 'B': '[30.988, 4.235]', 'C': '[69, 69]'}
        ],    # NOTE: we change arrays to strings which look like arrays, also, strings are single quoted.
    },
    {
        'name': '[8] Complex JSON: List of Objects in Object',
        'data':
            ('{"derp": [{"A": "Up"}, {"B": "Down"}]}\n'
             '{"derp": [{"A": "Yin"}, {"B": "Yang"}]}\n'
             '{"derp": [{"A": "Black"}, {"B": "White"}]}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {"A": "Up", "B": "Down"},
            {"A": "Yin", "B": "Yang"},
            {"A": "Black", "B": "White"}
        ]
    },
    {
        'name': '[9] Complex JSON: List of Objects with same Key Name',
        'data':
            ('{"derp": [{"B": "Up"}, {"B": "Down"}]}\n'
             '{"derp": [{"B": "Yin"}, {"B": "Yang"}]}\n'
             '{"derp": [{"B": "Black"}, {"B": "White"}]}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {"B": "['Up', 'Down']"},
            {"B": "['Yin', 'Yang']"},
            {"B": "['Black', 'White']"}
        ]    # Well, I don't really like this, "$.derp[0:].B"
    },
    {
        'name': '[11] Complex JSON: List with mixed types: Int, Str, Obj',
        'data': ('{"A": [123, "foo", {"B": "bar"}]}\n'
                 '{"A": [456, "goo", {"B": "car"}]}\n'
                 '{"A": [789, "hoo", {"B": "dar"}]}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {'B': 'bar'},
            {'B': 'car'},
            {'B': 'dar'}
        # Mapping: "$.A[0:].B"
        ],    # NOTE: AWS apparently just ignors the first two elements of the list
    },
    {
        'name': '[12] Complex JSON: List of Objects',
        'data':
            ('{"derp": [{"A": "Up"}, {"B": "Down"}]}\n'
             '{"derp": [{"A": "Yin"}, {"B": "Yang"}]}\n'
             '{"derp": [{"A": "Black"}, {"B": "White"}]}\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_JSONL,
        'expected_rows': [
            {"A": "Up", "B": "Down"},
            {"A": "Yin", "B": "Yang"},
            {"A": "Black", "B": "White"}
        ]
    },
    {
        'name': 'Complex CSV: Default Options: Duplicate column names takes last value',
        'data': ('foo,bar,foo,car\n'
                 '1,2,"dup",4\n'
                 '9,0,"dup",2\n').encode('utf-8'),
        'input_serial': INPUT_SERIAL_CSV,
        'expected_rows': [
            {'BAR': 2, 'FOO': 'dup', 'CAR': 4},
            {'BAR': 0, 'FOO': 'dup', 'CAR': 2},
        ]
    # XXX Kinesis only shows three columns existing. S3 Select returns all four but
    # AWS fails when we try to do a select on a column with a duplicate name.
    # botocore.exceptions.ClientError: An error occurred (AmbiguousFieldName) when calling the SelectObjectContent operation: Some field name in the query matches to multiple fields in the file. Please check the file and query, and try again.
    },
]


##
# Actual Tests
###############################################################################


##
# Dataset checks - mostly for casting/parser correctness
#########################################################

def do_dataset_icv_check(
        client, session, workbook, target_name, temp_path, test_name,
        file_gen, file_suffix, input_serial, schema, expected_rows, expected_icvs):

    # XXX FIXME XXX FIXME
    # Graduate me to a new load test.
    # Graduate me to a new load test.
    # Graduate me to a new load test.
    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen, file_suffix)

    with temp_dir_ctx as source_args:
        file_list = source_args["paths"]
        base_dir = source_args["path"]
        loadplan_ctx = loadplan_context(
            client, target_name, base_dir, file_list,
            input_serial, schema, file_name_pattern=f"*.{file_suffix}")

        with loadplan_ctx as (lp_source_args, parser_fn_name, parser_args):
            # Hrmm.. lp_source_args is just source_args with path changed
            dataset_ctx = dataset_context(
                workbook, lp_source_args, parser_fn_name, parser_args)

            with dataset_ctx as dataset:
                validate_load_dataset_records(
                    expected_rows, expected_icvs, dataset.records())


def do_cast_boolean_check(client, session, workbook, target_name, temp_path):
    test_name = "cast_boolean_check"
    file_suffix = ".csv"
    input_serial = INPUT_SERIAL_CSV
    boolean_printf = "ERROR: {col_name}: DfBoolean({field_val}) is invalid"

    header_file_data = b'A\n'
    success_file_data = (
        b'True\n'
        b'tRuE\n'  # Case check
        b'fAlSE\n'  # More case check and False
        b'""\n')  # check for FNF (None)
    icv_file_data = (
        b'asd\n'  # short str check
        b'asdfasdf\n'  # long str check
        b'12345\n')  # In range but invalid check
    expected_rows = [
        {"A": True},
        {"A": True},
        {"A": False},
        {"A": None}
    ]
    expected_icvs = [
        boolean_printf.format(col_name="A", field_val="asd"),
        boolean_printf.format(col_name="A", field_val="asdfasdf"),
        boolean_printf.format(col_name="A", field_val="12345")
    ]
    schema = {
        "rowpath": "$",
        "columns": [{"name": "A", "type": "DfBoolean", "mapping": "$.A"}]
    }

    file_data = b''.join([
        header_file_data,
        success_file_data,
        icv_file_data
    ])

    def file_gen():
        yield file_data

    do_dataset_icv_check(client, session, workbook, target_name, temp_path,
                         test_name, file_gen(), file_suffix, input_serial,
                         schema, expected_rows, expected_icvs)


def do_cast_integer_check(client, session, workbook, target_name, temp_path):
    test_name = "cast_boolean_check"
    file_suffix = ".csv"
    input_serial = INPUT_SERIAL_CSV
    int_non_digit_printf = (
        "ERROR: {col_name}: DfInt64({field_val}) contains "
        "non-digit char(0x{char:02x}) at {pos}")
    int_exceeds_range_printf = (
        "ERROR: {col_name}: DfInt64({field_val}) exceeds range")

    header_file_data = b'A\n'
    success_file_data = (
        b'1234\n'
        b'+2345\n'  # plus signs work
        b'-3456\n'  # negative signs work
        b'-9223372036854775808\n'  # long long min
        b'9223372036854775807\n'  # long long max
        b'""\n')  # FNF (None) check
    icv_file_data = (
        b'9223372036854775808\n'  # long long max + 1
        b'-9223372036854775809\n'  # long long min - 1
        b'asdf\n'  # All non-digit chars
        b'12345asdf\n')  # trailing non-digit chars
    expected_rows = [
        {"A": 1234},
        {"A": 2345},
        {"A": -3456},
        {"A": -9223372036854775808},
        {"A": 9223372036854775807},
        {"A": None}
    ]
    expected_icvs = [
        int_exceeds_range_printf.format(
            col_name="A", field_val="9223372036854775808"),
        int_exceeds_range_printf.format(
            col_name="A", field_val="-9223372036854775809"),
        int_non_digit_printf.format(
            col_name="A", field_val="asdf", char=ord('a'), pos='0'),
        int_non_digit_printf.format(
            col_name="A", field_val="12345asdf", char=ord('a'), pos='5'),
    ]
    schema = {
        "rowpath": "$",
        "columns": [{"name": "A", "type": "DfInt64", "mapping": "$.A"}]
    }

    file_data = b''.join([
        header_file_data,
        success_file_data,
        icv_file_data
    ])

    def file_gen():
        yield file_data

    do_dataset_icv_check(client, session, workbook, target_name, temp_path,
                         test_name, file_gen(), file_suffix, input_serial,
                         schema, expected_rows, expected_icvs)


def do_cast_float_check(client, session, workbook, target_name, temp_path):
    test_name = "cast_boolean_check"
    file_suffix = ".csv"
    input_serial = INPUT_SERIAL_CSV
    int_non_digit_printf = (
        "ERROR: {col_name}: DfFloat64({field_val}) contains "
        "non-digit char(0x{char:02x}) at {pos}")
    int_exceeds_range_printf = (
        "ERROR: {col_name}: DfFloat64({field_val}) exceeds range")

    # 1.797693134862315708145274237317043567981e+308
    float64_max = 2**1023 * (2**53 - 1) / 2**52
    # 4.940656458412465441765687928682213723651e-324
    float64_min = -1 / 2**(1023 - 1 + 52)
    float64_max_str = format(float64_max, '.1f')
    float64_min_str = format(float64_min, '.364f')  # 324 + 40 chars
    # XXX I wasn't able to push up against the lower bound
    # of float. However, we are successfully testing the
    # failure condition here, so I think this is fine.

    # Changing the first digit from 1 to 2 causes inf
    float64_max_exceeds_str = '2' + float64_max_str[1:]

    header_file_data = b'A\n'
    success_file_data = (
        b'1234.1\n'
        b'+2345.1\n'  # plus signs work
        b'-3456.1\n'  # negative signs work
        b'1234\n'   # no periods work
        b'""\n'  # FNF (None) check
    )
    success_file_data = success_file_data + b''.join(
        [float64_max_str.encode('utf-8'), b'\n'])
    icv_file_data = (
        b'asdf\n'  # All non-digit chars
        b'123.234abc\n'  # period with trailing non-digit chars
        b'123.234.345\n'  # double period detected
        b'12345asdf\n')  # no period with trailing non-digit chars
    icv_file_data = icv_file_data + b''.join(
        [float64_max_exceeds_str.encode('utf-8'), b'\n'])
    icv_file_data = icv_file_data + b''.join(
        [float64_min_str.encode('utf-8'), b'\n'])
    expected_rows = [
        {"A": 1234.1},
        {"A": 2345.1},
        {"A": -3456.1},
        {"A": 1234.0},
        {"A": None},
        {"A": float64_max}
    ]
    expected_icvs = [
        int_non_digit_printf.format(
            col_name="A", field_val="asdf", char=ord('a'), pos='0'),
        int_non_digit_printf.format(
            col_name="A", field_val="123.234abc", char=ord('a'), pos='7'),
        int_non_digit_printf.format(
            col_name="A", field_val="123.234.345", char=ord('.'), pos='7'),
        int_non_digit_printf.format(
            col_name="A", field_val="12345asdf", char=ord('a'), pos='5'),
        int_exceeds_range_printf.format(
            col_name="A", field_val=float64_max_exceeds_str),
        int_exceeds_range_printf.format(
            col_name="A", field_val=float64_min_str)
    ]
    schema = {
        "rowpath": "$",
        "columns": [{"name": "A", "type": "DfFloat64", "mapping": "$.A"}]
    }

    file_data = b''.join([
        header_file_data,
        success_file_data,
        icv_file_data
    ])

    def file_gen():
        yield file_data

    do_dataset_icv_check(client, session, workbook, target_name, temp_path,
                         test_name, file_gen(), file_suffix, input_serial,
                         schema, expected_rows, expected_icvs)

##
# Full Load / Final Table Checks
####################################


def do_full_load_user(client, session, target_name, temp_path, test_user):
    test_name = "test_full_load_user"

    file_type = "CSV"
    file_suffix = ".csv"
    data_desc = basic_data
    input_serial_json = json.dumps(INPUT_SERIAL_CSV)

    def single_file():
        yield get_file_from_data_desc(data_desc, file_type)

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, single_file(), file_suffix)

    with temp_dir_ctx as source_args:
        expected_rows = get_expected_rows_data_desc(data_desc, source_args['paths'])
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args, test_user)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_rows, load_tables['data'].records())


def do_full_load_types(client, session, target_name, temp_path, test_case):
    test_name = "test_full_load_types"

    file_type = test_case["type"]
    file_suffix = test_case["suffix"]
    data_desc = basic_data
    input_serial_json = json.dumps(test_case["input_serial"])

    def single_file():
        yield get_file_from_data_desc(data_desc, file_type)

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, single_file(), file_suffix)

    with temp_dir_ctx as source_args:
        expected_rows = get_expected_rows_data_desc(data_desc, source_args['paths'])
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_rows, load_tables['data'].records())


def do_full_load_functionality(client, session, target_name, temp_path, test_case):
    test_name = "test_full_load_functionality"

    # Parameters for test
    file_suffix = ".file"  # FIXME Not really needed?
    expected_rows = test_case["expected_rows"]
    input_serial_json = json.dumps(test_case["input_serial"])

    def file_gen():
        yield test_case["data"]

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    with temp_dir_ctx as source_args:
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_rows, load_tables['data'].records())


def do_full_load_formatting(client, session, target_name, temp_path, test_case):
    test_name = "test_full_load_functionality"

    # Parameters for test
    file_suffix = ".file"  # FIXME Not really needed?
    expected_rows = test_case["expected_rows"]
    input_serial_json = json.dumps(test_case["input_serial"])

    def file_gen():
        yield test_case["data"]

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    with temp_dir_ctx as source_args:
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_rows, load_tables['data'].records())


def do_example_of_arbitrary_file_list_with_schema(
        client, session, workbook, target_name, temp_path):
    test_name = "ttucker_testing_stuff"
    file_suffix = ".ttucker"

    file_data = b'{"A": "true", "B": 123, "C": 0.7, "D": "hey you"}'
    input_serial = INPUT_SERIAL_JSONL
    schema = {
        "rowpath": "$",
        "columns": [
            {"name": "A", "type": "DfBoolean", "mapping": "$.A"},
            {"name": "B", "type": "DfInt64", "mapping": "$.B"},
            {"name": "C", "type": "DfFloat64", "mapping": "$.C"},
            {"name": "D", "type": "DfString", "mapping": "$.D"}
        ]
    }

    def file_gen():
        yield file_data

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    # {'A': True, 'B': 123, 'C': 0.7, 'D': 'hey you', '_X_FILE_RECORD_NUM': 1, '_X_ICV': '', '_X_SOURCE_DATA': '', '_X_PATH': '/xcfield/xcalar.qa/load_wizard_temp/99fe2902dfd524a9ee167b50050fa389_184aa42e-4698-4a9f-8cb6-a887975856d3_file_0.ttucker'}
    with temp_dir_ctx as source_args:
        file_list = source_args["paths"]
        base_dir = source_args["path"]
        loadplan_ctx = loadplan_context(
            client, target_name, base_dir, file_list,
            input_serial, schema, file_name_pattern=f"*.{file_suffix}")

        with loadplan_ctx as (lp_source_args, parser_fn_name, parser_args):
            # Hrmm.. lp_source_args is just source_args with path changed
            dataset_ctx = dataset_context(
                workbook, lp_source_args, parser_fn_name, parser_args)

            with dataset_ctx as dataset:
                # info = dataset.info()
                # info[columns] = [{"name": . "type": . }, ...]
                for rec in dataset.records():
                    print(f"{rec}")
    return


def do_discover_all_arrays(client, session, target_name, temp_path):
    test_name = "test_discover_all_arrays"
    # ENG-8944 - We want to make sure that our message for invalid columns
    # appears in the discover_all() results because XD will use this.

    file_suffix = ".array.jsonl"
    input_serial_json = json.dumps(INPUT_SERIAL_JSONL)

    # XXX I should make a generator which produces a file_gen and expected rows.
    def file_gen():
        yield (b'{"A": 1, "B": 2, "C": 3}\n')
        yield (b'{"A": 4, "B": 5, "C": 6, "D": [1, 2, 3]}\n')
    expected_rows = [
        {"A": 1, "B": 2, "C": 3},
        {"A": 4, "B": 5, "C": 6}
    ]

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    with temp_dir_ctx as source_args:
        path = source_args["path"]
        file_name_pattern = source_args["fileNamePattern"]
        recursive = source_args["recursive"]

        discover_table_contexts = do_discover_all(
            test_name, client, session.name, target_name, path,
            file_name_pattern, recursive, input_serial_json)

        with contextlib.ExitStack() as stack:
            discover_tables = {nn: stack.enter_context(cc)
                               for nn, cc in discover_table_contexts.items()}

            statuses = [json.loads(rr["STATUS"])
                        for rr in discover_tables["results"].records()]

            found_fail = False
            found_pass = False
            assert len(statuses) == 2
            for status in statuses:
                if len(status['unsupported_columns']) == 1:
                    found_fail = True
                if len(status['unsupported_columns']) == 0:
                    found_pass = True
            assert found_pass
            assert found_fail
            print("Found pass and fail messages")


def do_full_load_arrays(client, session, target_name, temp_path):
    # ENG-8944 - We should remove the columns with arrays
    test_name = "test_full_load_arrays"

    file_suffix = ".array.jsonl"
    input_serial_json = json.dumps(INPUT_SERIAL_JSONL)

    def file_gen():
        yield (b'{"A": 1, "B": 2, "C": 3}\n')
        yield (b'{"A": 4, "B": 5, "C": 6, "D": [1, 2, 3]}\n')
    expected_rows = [
        {"A": 1, "B": 2, "C": 3},
        {"A": 4, "B": 5, "C": 6}
    ]

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    with temp_dir_ctx as source_args:
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_rows, load_tables['data'].records())


def do_full_load_csv_duplicate_columns(client, session, target_name, temp_path):
    # We remove columns with duplicate names in csv source data
    test_name = "test_full_load_duplicate_source_columns"

    file_suffix = ".dup.csv"
    input_serial_json = json.dumps(INPUT_SERIAL_CSV)

    def file_gen():
        yield (b'A,B,B,C\n'
               b'1,2,3,4\n')

    expected_rows = [
        {"A": 1, "C": 4},
    ]
    # XXX Make error names globals and inlcude them here.
    # Commented because of ENG-9162 - Need to impl better testing for discover tables
    # expected_unsupported_columns = [
    #     {"name": "B", "mapping": "$.B", "message": "Source data contains duplicate columns."}
    # ]

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    with temp_dir_ctx as source_args:
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_rows, load_tables['data'].records())


def do_full_load_missing_and_extra_fields(client, session, target_name, temp_path):
    # If there are extra fields, we will remove them... :(
    # If a field isn't present, AWS will return an empty string. We cast non-string
    # fields to None's in the back end.
    # XXX Actually, this might be wrong... :( Need to setup tests for this
    # This whole test needs to determine what happens when each field type is missing
    test_name = "test_full_load_none_type"

    file_suffix = ".none.extra"
    input_serial_json = json.dumps(INPUT_SERIAL_CSV)

    # Note: rows must be unique for datasetCheck() to pass (I think?)
    header = "A,B,C\n"
    rows = [f"{ii},{ii},{ii}\n" for ii in range(S3_SELECT_LIMIT)]
    rows.append("{num},{num}\n".format(num=S3_SELECT_LIMIT))
    rows.append("{num},{num},{num},{num}\n".format(num=S3_SELECT_LIMIT + 1))

    expected_rows = [
        {"A": ii, "B": ii, "C": ii}
        for ii in range(S3_SELECT_LIMIT)
    ]
    expected_rows.append({"A": S3_SELECT_LIMIT,
                          "B": S3_SELECT_LIMIT})
    expected_rows.append({"A": S3_SELECT_LIMIT + 1,
                          "B": S3_SELECT_LIMIT + 1,
                          "C": S3_SELECT_LIMIT + 1})

    def file_gen():
        yield f"{header}{''.join(rows)}".encode('utf-8')

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    with temp_dir_ctx as source_args:
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_rows, load_tables['data'].records())


def do_full_load_source_data_column(client, session, target_name, temp_path):
    test_name = "test_full_load_source_data_column"

    file_suffix = ".source.data"
    input_serial_json = json.dumps(INPUT_SERIAL_CSV)

    # Note: rows must be unique for datasetCheck() to pass (I think?)
    header = "A,B,C\n"
    rows = [f"{ii},{ii},{ii}\n" for ii in range(S3_SELECT_LIMIT)]
    rows.append("{num},{num},c\n".format(num=S3_SELECT_LIMIT))

    expected_data_rows = [
        {"A": ii, "B": ii, "C": ii}
        for ii in range(S3_SELECT_LIMIT)
    ]
    # "<col_name>: <col_type>(<val>) contains non-digit char(0x<\xchar>) at <offset>"
    icv_message = "ERROR: C: DfInt64(c) contains non-digit char(0x63) at 0"
    expected_comp_rows = [
        {ICV_COLUMN_NAME: icv_message,
         SRC_COLUMN_NAME: (f"OrderedDict([('A', '{S3_SELECT_LIMIT}'), "
                           f"('B', '{S3_SELECT_LIMIT}'), ('C', 'c')])"),
         FRN_COLUMN_NAME: S3_SELECT_LIMIT + 1}
    ]

    def file_gen():
        yield f"{header}{''.join(rows)}".encode('utf-8')

    temp_dir_ctx = get_test_temp_directory(
        test_name, target_name, temp_path, file_gen(), file_suffix)

    with temp_dir_ctx as source_args:
        load_tables_ctx = do_full_load(
            client, session, test_name, input_serial_json, source_args)

        with contextlib.ExitStack() as stack:
            load_tables = {nn: stack.enter_context(cc)
                           for nn, cc in load_tables_ctx.items()}
            validate_output_table_records(expected_data_rows, load_tables['data'].records())
            # XXX Should probably make a function which checks ICV columns...
            validate_output_table_records(expected_comp_rows, load_tables['comp'].records())


##
# Parametrize Tests
########################

##
# Local Tests


#@pytest.mark.parametrize("target_name,temp_path", LOCAL_TARGETS)
#def test_local_discover_all_loop(target_name, temp_path):
#    with setup_load_wizard() as (client, session, workbook):
#        do_discover_all_loop(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", LOCAL_TARGETS)
#def test_local_schema_size_exceed_field_size(target_name, temp_path):
#    with setup_load_wizard() as (client, session, workbook):
#        do_schema_size_exceed_file_size(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", LOCAL_TARGETS)
#@pytest.mark.parametrize("test_user", [None, "test_user"])
#def test_local_full_load_user(target_name, temp_path, test_user):
#    with setup_load_wizard(test_user) as (client, session, workbook):
#        do_full_load_user(client, session, target_name, temp_path, test_user)


#@pytest.mark.parametrize("target_name,temp_path", LOCAL_TARGETS)
#@pytest.mark.parametrize("test_case", local_file_formats, ids=lambda case: case["type"])
#def test_local_full_load_types(target_name, temp_path, test_case):
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_types(client, session, target_name, temp_path, test_case)


###
## Cloud Tests


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_discover_all_loop(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_discover_all_loop(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_schema_size_exceed_field_size(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_schema_size_exceed_file_size(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#@pytest.mark.parametrize("test_user", [None, "test_user"])
#def test_cloud_full_load_user(target_name, temp_path, test_user):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard(test_user) as (client, session, workbook):
#        do_full_load_user(client, session, target_name, temp_path, test_user)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#@pytest.mark.parametrize("test_case", local_file_formats, ids=lambda case: case["type"])
#def test_cloud_full_load_types(target_name, temp_path, test_case):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_types(client, session, target_name, temp_path, test_case)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#@pytest.mark.parametrize("test_case", functionality_cases, ids=lambda case: case["name"])
#def test_cloud_full_load_functionality(target_name, temp_path, test_case):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_functionality(client, session, target_name, temp_path, test_case)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#@pytest.mark.parametrize("test_case", formatting_tests, ids=lambda case: case["name"])
#def test_cloud_full_load_formatting(target_name, temp_path, test_case):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_formatting(client, session, target_name, temp_path, test_case)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_full_load_missing_and_extra_fields(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_missing_and_extra_fields(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_discover_all_arrays(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_discover_all_arrays(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_full_load_arrays(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_arrays(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_full_load_csv_duplicate_columns(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_csv_duplicate_columns(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_full_load_source_data_column(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_full_load_source_data_column(client, session, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_cast_boolean_check(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_cast_boolean_check(client, session, workbook, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_cast_integer_check(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_cast_integer_check(client, session, workbook, target_name, temp_path)


#@pytest.mark.parametrize("target_name,temp_path", CLOUD_TARGETS)
#def test_cloud_cast_float_check(target_name, temp_path):
#    if not cloud_tests_enabled():
#        pytest.skip(CLOUD_TESTS_DISABLED_MSG)
#    with setup_load_wizard() as (client, session, workbook):
#        do_cast_float_check(client, session, workbook, target_name, temp_path)

# yapf: enable
