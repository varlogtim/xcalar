#!/usr/bin/env python3
import os
import re
import sys
import json
import hashlib
import logging
import argparse
import time
from datetime import datetime

from concurrent import futures
from contextlib import contextmanager

from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.dataflow import Dataflow

from xcalar.container.util import cleanse_column_name
from xcalar.container.schema_discover.aws_parser import add_compression_type
from xcalar.container.target.manage import get_target_data, build_target

from xcalar.container.loader.load_wizard_load_plan import LoadWizardLoadPlanBuilder
from xcalar.container.schema_discover.schema_discover_dataflows import (
    get_load_table_query_string)

# Operation:
# There are two functions:
#  1. Discovery
#   - Concurrently lists files in path matching pattern
#   - Samples a number of records from each file
#   - Find the superset schema of sample rows in file.
#   - Builds a metadata file for each file which contains load information
#  2. Load
#   - Reads metadata files from given path
#   - Loads tables with information in metadatafiles.
#   Notes: If publish table exists, it will be unpublished first.

logger = logging.getLogger('xcalar')
logger.setLevel(logging.DEBUG)

# XXX Should have different loggers per command/context
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - BulkSchemaLoad: thread_id(%(thread)d): %(levelname)s: %(message)s'
    )
    handler.setFormatter(formatter)

    fileHandler = logging.FileHandler("./batch_csv_load.log")
    fileHandler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.addHandler(fileHandler)

NUM_ROWS_TO_ANALYSE = 100
NUM_ERROR_REC_TO_PRINT = 10
TARGET_NAME = 'Xcalar S3 Connector'

#
# Constants
#

SESSION_NAME = "BatchSchemaDiscover"
USER_NAME = "admin"

DF_UNKNOWN = "DfUnknown"
DF_INT = "DfInt64"
DF_BOOL = "DfBoolean"
DF_FLOAT = "DfFloat64"
DF_STRING = "DfString"
DF_ARRAY = "DfArray"

MUST_BE_CSV_EXT = True

#
# Helper Functions
#

# Note: There are quite a few functions that should be moved to a shared location.
# This will be done in the future. This script needs to be self-contained/portable.


def list_only_files(target, **args):
    # Wrapper for old/new list files.
    ret = target.list_files(**args)
    if isinstance(ret, dict):
        file_names = [
            item['name'] for item in ret['files'] if not item['isDir']
        ]
        return file_names
    if isinstance(ret, list):
        file_names = [
            item['name'] for item in ret if not item['attr']['isDirectory']
        ]
        return file_names


# XXX Function should be in shared location
def escape_key_for_mapping(key):
    escaped_key = ''.join([f'"{cc}' if cc == '"' else cc for cc in str(key)])
    return f'"{escaped_key}"'


# XXX Function should be in shared location
# This allows duplicate keys where a dict does not
# E.g., '{"A": 1, "A": 2}' -> [('A', 1), ('A', 2)]
def json_loads_tuple_list(json_str):
    def obj_pairs_hook(obj):
        return obj

    row_tuple_list = json.loads(json_str, object_pairs_hook=obj_pairs_hook)
    return row_tuple_list


def get_load_id(idx):
    # LOAD_WIZARD_5F89ECAD0B79EE28_1602849342_359853_1
    load_id_printf = "LOAD_WIZARD_AAAABBBBCCCCDDDD_{secs}_{msecs}_{idx}"

    datetime_now = datetime.now()
    datetime_epoch = datetime.utcfromtimestamp(0)
    time_delta = (datetime_now - datetime_epoch)
    seconds_since_epoch = time_delta.total_seconds()

    epoch_micro_parts = str(seconds_since_epoch).split('.')
    seconds = epoch_micro_parts[0].ljust(10, '0')
    microsecs = epoch_micro_parts[1].ljust(6, '0')

    load_id = load_id_printf.format(secs=seconds, msecs=microsecs, idx=idx)
    return load_id


# Should put these in a util.py somewhere.
int_regex = re.compile(r'^(-|\+)?[0-9]+$')
float_regex = re.compile(r'^(-|\+)?([0-9]+)?\.[0-9]+$')


# XXX Function should be in shared location
def type_guess(obj):
    if obj is None:
        return DF_STRING
    # Booleans must be checked first
    if isinstance(obj, bool):
        return DF_BOOL
    if isinstance(obj, int):
        return DF_INT
    if isinstance(obj, float):
        return DF_FLOAT
    if isinstance(obj, str):
        if obj.lower() in ["false", "true"]:
            return DF_BOOL
        if re.match(int_regex, obj):
            return DF_INT
        if re.match(float_regex, obj):
            return DF_FLOAT
    return DF_STRING


# XXX Function should be in shared location
def row_gen(target, path, input_serial, num_rows):
    query = f'SELECT * FROM s3Object s LIMIT {num_rows}'
    add_compression_type(input_serial, path)
    target_data = get_target_data(target.name)
    target_internal = build_target(target.name, target_data, path)
    with target_internal.open(path, 's3select') as s3select:
        yield from s3select.rows(input_serial, query)


# XXX Function should be in shared location
def get_schema_csv(row_tuple_list):
    if not isinstance(row_tuple_list, list):
        raise ValueError(f"Row must be list, not {type(row_tuple_list)}")
    columns = []

    for row_tuple in row_tuple_list:
        if not isinstance(row_tuple, tuple):
            raise ValueError(f"Row must be a list of tuples")

        key, val = row_tuple
        col_name = cleanse_column_name(key, True)
        escaped_key = escape_key_for_mapping(key)
        col_mapping = f'$.{escaped_key}'

        if isinstance(val, dict) or isinstance(val, list) or isinstance(
                val, tuple):
            raise ValueError(f"Row cannot contain: {type(val)}")
        if key == "":
            # XXX Should we just log errors?
            raise ValueError(f"Trailing columns detected with value({val})")
        columns.append({
            "name": col_name,
            "mapping": col_mapping,
            "type": type_guess(val)
        })
    return columns


# XXX Function should be in shared location
def get_inclusive_schema(row_gen):
    master_schema = None
    for row_num, row in enumerate(row_gen):
        row_tuple_list = json_loads_tuple_list(row)
        columns = get_schema_csv(row_tuple_list)
        if master_schema is None:
            master_schema = columns
            continue
        prev_types = [col['type'] for col in master_schema]
        curr_types = [col['type'] for col in columns]
        if len(prev_types) != len(curr_types):
            raise ValueError("Inner row mismatch")
        for col_idx, curr_type in enumerate(curr_types):
            if curr_type == DF_STRING or prev_types[col_idx] == DF_STRING:
                master_schema[col_idx]['type'] = DF_STRING
                continue

            if curr_type == DF_FLOAT and prev_types[col_idx] == DF_INT:
                master_schema[col_idx]['type'] = DF_FLOAT
                continue
            if curr_type == DF_FLOAT and prev_types[col_idx] != DF_FLOAT:
                master_schema[col_idx]['type'] = DF_STRING
                continue

            if curr_type == DF_INT and prev_types[col_idx] == DF_FLOAT:
                master_schema[col_idx]['type'] = DF_FLOAT
                continue
            if curr_type == DF_INT and prev_types[col_idx] != DF_INT:
                master_schema[col_idx]['type'] = DF_STRING
                continue

            if curr_type == DF_BOOL and prev_types[col_idx] != DF_BOOL:
                master_schema[col_idx]['type'] = DF_STRING
                continue
    return master_schema


def write_pub_table_query_to_kvstore(client, query, pub_table_name):
    kvs = client.global_kvstore()

    key = f"/ui/tblMeta/{pub_table_name}"
    value = query
    persist = True
    kvs.add_or_replace(key, value, persist)


def remove_export(query_str_in):
    ###
    # Remove last Export Op, and get the source table name.
    load_query = None
    try:
        load_query = json.loads(query_str_in)
    except Exception as e:
        raise ValueError("Unable to decode load_query: {e}")

    if not isinstance(load_query, list):
        raise ValueError("load_query must be list")

    last_load_item = load_query.pop()
    if not isinstance(last_load_item, dict):
        raise ValueError("query list items must be dicts")

    last_load_op = last_load_item.get("operation", "")
    if last_load_op != "XcalarApiExport":
        raise ValueError("input query must end with XcalarApiExport op")

    return json.dumps(load_query)


#
# Concurrency Functions
#

POISONED = False

MAX_LOAD_RETRIES = 5
RETRY_TIMEOUT = 1


def poisoned():
    return POISONED


def poison_everyone():
    global POISONED
    POISONED = True


@contextmanager
def main_ctx():
    log_prefix = f"MAIN CONTEXT: "

    def my_debug(msg):
        logger.debug(f"{log_prefix}{msg}")

    try:
        my_debug("Starting")
        # client = Client(url=node_0_url, client_secrets=client_secrets)
        client = Client(bypass_proxy=True)
        my_debug("Acquired client connection")

        workbook_name = f"{SESSION_NAME}_MAIN"
        workbook = client.create_workbook(workbook_name)
        my_debug(f"Created workbook: {workbook_name}")

        session = workbook.activate()
        my_debug(f"Activated session: {workbook_name}")

        yield (client, session)
    except Exception as e:
        logger.error(f"{log_prefix}Encountered an exception: {e}")
        raise
    finally:
        my_debug("Ending")
        try:
            workbook.delete()    # This destroys session?
            logger.debug(f"Deleted workbook: {workbook_name}")
        except Exception:
            pass


@contextmanager
def inner_ctx(container_session_name):
    log_prefix = f"INNER CONTEXT: "

    # XXX Need to return a logger for this.
    def my_debug(msg):
        logger.debug(f"{log_prefix}{msg}")

    try:
        my_debug("Starting")
        client = Client(bypass_proxy=True)
        my_debug("Acquired client connection")

        api = XcalarApi(bypass_proxy=True)
        my_debug("Acquired API connection")

        session = client.get_session(container_session_name)
        my_debug(f"Acquired session: {container_session_name}")

        api.setSession(session)
        my_debug(f"Set API to session: {container_session_name}")

        op = Operators(api)
        my_debug("Acquired Operators")
        yield (client, session, op)
    except Exception as e:
        logger.error(f"{log_prefix}Encountered exception: {e}")
    finally:
        my_debug("Ending")
        api.setSession(None)


# XXX It would be good if we had loggers for each context.
@contextmanager
def table_ctx(client, session, table_name, query_name, query, opt_query):
    table = None
    log_prefix = f"TABLE({table_name}): "

    def my_debug(msg):
        logger.debug(f"{log_prefix}{msg}")

    try:
        my_debug("Starting temp table build")
        params = {"session_name": session.name, "user_name": USER_NAME}
        dataflow = Dataflow.create_dataflow_from_query_string(
            client, query, optimized_query_string=opt_query)
        my_debug("Built temp table query")

        for retry in range(1, MAX_LOAD_RETRIES+1):
            try:
                session.execute_dataflow(
                    dataflow,
                    table_name=table_name,
                    query_name=query_name,
                    params=params,
                    optimized=True,
                    is_async=False)
                my_debug("Executed temp table query")
                break
            except Exception as e:
                if retry >= MAX_LOAD_RETRIES:
                    raise e
                logger.error(f"{log_prefix}Encountered exception: {e}. Retry {retry}.")
                time.sleep(RETRY_TIMEOUT)

        table = session.get_table(table_name)
        my_debug("Acquired temp table")
        yield table
    except Exception as e:
        logger.error(f"{log_prefix}Encountered exception: {e}")
        raise
    finally:
        my_debug("Cleaning up")
        if table is not None:
            table.drop()
            my_debug("Temp table dropped")


# Load


def load_single(client, session, op, target_name, metadata_path,
                stop_on_error_table, allow_failures):
    response = {"success": False, "full_path": "unknown"}

    if poisoned():
        logger.debug("poisoned (x_x)")
        return response

    try:
        target_data = get_target_data(target_name)
        target_internal = build_target(target_name, target_data, '/')

        # Download and Read Metadata
        with target_internal.open(metadata_path, 'rb') as metadata_file:
            metadata_bytes = metadata_file.read()
            metadata_json = metadata_bytes.decode('utf-8')
            metadata = json.loads(metadata_json)

        source_file_path = metadata['path']
        response["full_path"] = source_file_path

        # Remove UDF if it exists and upload new one from metadata
        try:
            # Remove UDF if it exists
            client.get_udf_module(metadata['udf_module_name']).delete()
            logger.debug(f"UDF module existed: {metadata['udf_module_name']}")
        except ValueError as e:
            logger.debug(
                f"UDF module did not exist: {metadata['udf_module_name']}")
            pass
        udf = client.create_udf_module(metadata['udf_module_name'],
                                 metadata['udf_source'])
        logger.debug(f"Created UDF module: {metadata['udf_module_name']}")

        # Get table names and queries
        pub_table_name = metadata['pub_table_name']

        load_query = metadata['query']
        load_opt_query = metadata['query_optimized']
        load_table_name = metadata['table_name']
        load_query_name = metadata['query_name']

        load_table_ctx = table_ctx(client, session, load_table_name,
                                   load_query_name, load_query, load_opt_query)
        with load_table_ctx as load_table:

            try:
                op.unpublish(pub_table_name)
                logger.debug("Table was published, unpublished it: "
                             f"{pub_table_name}")
            except Exception:
                pass
            op.publish(load_table_name, pub_table_name)
            logger.info(f"Published table: {pub_table_name}")

            # Join load and data queries, save to KVS
            restore_query = remove_export(load_query)
            write_pub_table_query_to_kvstore(client, restore_query,
                                             pub_table_name)
            logger.info(f"Wrote KVS entry for: {pub_table_name}")

            response['success'] = True

        #udf.delete()
        return response

    except Exception as e:
        if not allow_failures:
            logger.exception(
                "Single Load encountered an exception "
                f"when loading: {metadata_path} - POISONING EVERYONE")
            poison_everyone()
            return response
        else:
            logger.exception("Single Load encountered an exception "
                             f"when loading: {metadata_path} - ERRORS ALLOWED")
            return response


def load_group(session_name, target_name, metadata_paths, stop_on_error_table,
               allow_failures):
    with inner_ctx(session_name) as (client, session, op):
        return [
            load_single(client, session, op, target_name, metadata_path,
                        stop_on_error_table, allow_failures)
            for metadata_path in metadata_paths
        ]


def load_concurrent(target_name, metadata_dir, num_threads,
                    stop_on_error_table, allow_failures):
    with main_ctx() as (client, session):
        target = client.get_data_target(target_name)
        logger.debug(
            f"Listing files on target: '{target_name}' at '{metadata_dir}'")

        file_names = list_only_files(target, path=metadata_dir)
        logger.info(
            f"Found {len(file_names)} metadata files in {metadata_dir}")

        full_file_names = [
            os.path.join(metadata_dir, file_name) for file_name in file_names
        ]
        file_groups = [
            full_file_names[group_id::num_threads]
            for group_id in range(num_threads)
        ]

        with futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            logger.debug(f"Entered ThreadPoolExecutor")
            load_group_threads = [
                executor.submit(load_group, session.name, target_name,
                                file_group, stop_on_error_table,
                                allow_failures) for file_group in file_groups
            ]

            num_failures = 0
            num_success = 0

            for group_response in futures.as_completed(load_group_threads):
                single_results = group_response.result()
                # [{"success": True, "full_path": ""}, ... ]
                for single_result in single_results:
                    if single_result["success"]:
                        num_success = num_success + 1
                    else:
                        num_failures = num_failures + 1

            if num_failures == 0:
                logger.info(
                    f"SUCCESS!!! All {num_success} DATA "
                    "tables were successfully created with no error tables")
            else:
                logger.error(
                    f"ERROR!!! There were {num_failures} failures "
                    f"and {num_success} successes when loading tables")


# Discover


def discover_single(index, client, target, base_path, file_path, input_serial,
                    metadata_dir):

    full_path = os.path.join(base_path, file_path)
    response = {"success": False, "full_path": full_path}

    if poisoned():
        logger.debug("poisoned (x_x)")
        return response

    try:
        if MUST_BE_CSV_EXT:
            _, file_ext = os.path.splitext(full_path)
            if file_ext != ".csv":
                raise ValueError(
                    "File found at: {full_path} does not end in '.csv'")

        # Get row generator
        gen = row_gen(target, full_path, input_serial, NUM_ROWS_TO_ANALYSE)

        # Find Schema of given rows
        columns = get_inclusive_schema(gen)
        schema = {"rowpath": "$", "columns": columns}

        # Get Fake Load Id
        load_id = get_load_id(index)

        load_tn = f"xl_{load_id}_load"
        load_qn = f"q_{load_id}_load"

        # Make Source Args
        source_args = [{
            "targetName": target.name,
            "path": full_path,
            "recursive": False,
            "fileNamePattern": os.path.basename(file_path)
        }]

        input_serial_json = json.dumps(input_serial)

        # Make LoadPlan
        load_plan_builder = LoadWizardLoadPlanBuilder()
        load_plan_builder.set_schema_json(json.dumps(schema))
        load_plan_builder.set_load_id(load_id)
        load_plan_builder.set_input_serial_json(input_serial_json)
        (udf_source, func_name) = load_plan_builder.get_load_plan()

        # Parametrize Parser Args with Load Plan
        module_name = f"LOAD_PLAN_UDF_{load_id}"
        parser_fn_name = f"{module_name}:{func_name}"
        parser_arg_json = json.dumps({
            'loader_name': 'LoadWizardLoader',
            'loader_start_func': 'load_with_load_plan'
        })

        # Build Dataflows with args
        (load_qs, load_opt_qs) = get_load_table_query_string(
            source_args, parser_fn_name, parser_arg_json, columns, load_tn)

        pub_table_name = cleanse_column_name(file_path, True)

        # Make Metadata Object
        metadata = {
            "path": full_path,
            "schema": schema,
            "pub_table_name": pub_table_name,
            "table_name": load_tn,
            "query_name": load_qn,
            "query": load_qs,
            "query_optimized": load_opt_qs,
            "udf_module_name": module_name,
            "udf_source": udf_source
        }

        # Get writable target for saving metadata objects
        target_data = get_target_data(target.name)
        target_internal = build_target(target.name, target_data, '/')

        # Build paths and serialize metadata
        hashed_path = hashlib.md5(full_path.encode('utf-8')).hexdigest()
        metadata_path = os.path.join(metadata_dir, hashed_path)
        ser_metadata = json.dumps(metadata).encode('utf-8')

        # Write Metadata Object
        with target_internal.open(metadata_path, 'wb') as f:
            f.write(ser_metadata)
            num_cols = len(schema['columns'])
            logger.info(
                f"SUCCESS: src='{full_path}', meta_path='{metadata_path}', "
                f"num_cols={num_cols}, table_name='{pub_table_name}'")

        response["success"] = True
        return response

    except Exception as e:
        logger.exception("Single discovery encountered an exception "
                         f"when loading: {full_path} - POISONING EVERYONE")
        poison_everyone()

        return response


def discover_group(index, target_name, source_base_path, source_file_paths,
                   input_serial, metadata_dir):
    # Each group needs their own client as they are not thread-safe
    client = Client(bypass_proxy=True)
    target = client.get_data_target(target_name)
    # Should get a response from every thread, always.
    return [
        discover_single(index, client, target, source_base_path, file_path,
                        input_serial, metadata_dir)
        for file_path in source_file_paths
    ]


def discover_concurrent(target_name, source_base_path, name_pattern, recursive,
                        metadata_dir, input_serial, max_num_files,
                        num_threads):
    with main_ctx() as (client, session):
        # Build Target and get file list
        target = client.get_data_target(target_name)
        file_names = list_only_files(
            target,
            path=source_base_path,
            pattern=name_pattern,
            recursive=recursive)

        num_found_files = len(file_names)
        logger.info(f"Found {num_found_files} files on "
                    f"{target_name} at {source_base_path}")

        if num_found_files == 0:
            raise ValueError("No files to scan")

        # Check if we have a file limit (for partial runs)
        num_files_to_scan = num_found_files
        if max_num_files < num_found_files:
            num_files_to_scan = max_num_files
            logger.info(f"Limiting files to scan to: {max_num_files}")
            file_names = file_names[:num_files_to_scan]

        # Limit concurrency if we don't have enough files to justify it.
        if num_files_to_scan < num_threads:
            logger.info(
                f"Limiting concurency to num files: {num_files_to_scan}")
            num_threads = num_files_to_scan

        # Split files into ~equal groups
        file_groups = [
            file_names[group_id::num_threads]
            for group_id in range(num_threads)
        ]

        # Launch a thread for each group of files we need to process
        successful_paths = []
        with futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            logger.debug(f"Entered ThreadPoolExecutor")
            # Submit num groups threads
            discover_group_threads = [
                executor.submit(discover_group, index, target_name, source_base_path,
                                file_groups[index], input_serial, metadata_dir)
                for index in range(len(file_groups))
            ]
            # Wait for response.
            # Note: all threads always return a value, whether pass/fail
            for group_response in futures.as_completed(discover_group_threads):
                results = group_response.result()
                # results: [{"success": False, "full_path": full_path}, ... ]
                [
                    successful_paths.append(result["full_path"])
                    for result in results if result["success"]
                ]

        # Check if successful
        if len(successful_paths) == num_files_to_scan:
            logger.info(f"SUCCESS!!! All {num_files_to_scan} "
                        "files were scanned successfully")
        else:
            logger.error(f"FAILURE!!! Only {len(successful_paths)} out of "
                         f"{num_files_to_scan} were successful :(")


#
# Argument Parsing
#


def check_target_exists(target_name):
    client = Client(bypass_proxy=True)
    target = client.get_data_target(target_name)


def check_metadata_path_empty(target_name, metadata_path):
    client = Client(bypass_proxy=True)
    target = client.get_data_target(target_name)
    files = list_only_files(target, path=metadata_path, recursive=True)
    # files = target.list_files(metadata_path)
    if len(files) != 0:
        raise ValueError(
            f"Metadata path is not empty, found {len(files)} files. "
            "Please specify another empty path to write metadata files.")


def check_if_target_is_writable(target_name, path):
    # Should check up front if we can write into the bucket?
    return


def build_args_for_load(args):
    load_args = {
        "target_name": None,
        "metadata_path": None,
        "num_threads": None,
        "allow_failures": None,
        "stop_on_table_error": None
    }
    target_name = args.target_name
    if target_name is None:
        raise ValueError("Must specify target_name")
    check_target_exists(target_name)
    load_args["target_name"] = target_name

    metadata_path = args.metadata_path
    if metadata_path is None:
        raise ValueError("Must specify metadata_path")
    load_args["metadata_path"] = metadata_path

    num_threads = args.num_threads
    try:
        num_threads = int(num_threads)
    except (ValueError, TypeError):
        raise ValueError("Num threads must be int")
    load_args["num_threads"] = num_threads

    allow_failures = args.allow_failures
    if allow_failures not in ["true", "false"]:
        raise ValueError(f"Stop on error must [true, false]")
    if allow_failures == "true":
        allow_failures = True
    else:
        allow_failures = False
    load_args["allow_failures"] = allow_failures

    stop_on_table_error = args.stop_on_table_error
    if stop_on_table_error not in ["true", "false"]:
        raise ValueError(f"Stop on error tables entries must [true, false]")
    if stop_on_table_error == "true":
        stop_on_table_error = True
    else:
        stop_on_table_error = False
    load_args["stop_on_table_error"] = stop_on_table_error

    # Just to be sure
    for key, val in load_args.items():
        if val is None:
            raise ValueError(f"Problem finding argument: {key}")

    return load_args


def build_args_for_discovery(args):
    discovery_args = {
        "target_name": None,
        "source_path": None,
        "metadata_path": None,
        "name_pattern": None,
        "recursive": None,
        "max_num_files": None,
        "num_threads": None,
        "input_serial": None
    }

    target_name = args.target_name
    if target_name is None:
        raise ValueError("Must specify target_name")
    check_target_exists(target_name)
    discovery_args["target_name"] = target_name

    source_path = args.source_path
    if source_path is None:
        raise ValueError("Must specify src_path")
    discovery_args["source_path"] = source_path

    metadata_path = args.metadata_path
    if metadata_path is None:
        raise ValueError("Must specify metadata_path")
    check_metadata_path_empty(target_name, metadata_path)
    discovery_args["metadata_path"] = metadata_path

    discovery_args["name_pattern"] = args.name_pattern

    recursive = args.recursive
    if recursive not in ["true", "false"]:
        err(schema_parser.format_help())
        raise ValueError(f"recursive option must be in [true, false]")
    if recursive == "true":
        recursive = True
    else:
        recursive = False
    discovery_args["recursive"] = recursive

    max_num_files = args.max_num_files
    try:
        max_num_files = int(max_num_files)
    except (ValueError, TypeError):
        raise ValueError("Max num files must be int")
    discovery_args["max_num_files"] = max_num_files

    num_threads = args.num_threads
    try:
        num_threads = int(num_threads)
    except (ValueError, TypeError):
        raise ValueError("Num threads must be int")
    discovery_args["num_threads"] = num_threads

    # Build input_serialization obj:
    input_serial = {"CSV": {}}

    file_header = args.file_header_info
    val_enum = ["USE", "IGNORE", "NONE"]
    if file_header is None or file_header.upper() not in val_enum:
        raise ValueError(
            f"file header must be in: {val_enum}, not {file_header}")
    input_serial["CSV"]["FileHeaderInfo"] = file_header

    # Docs say "single character" ... but multiple chars do work, e.g., \r\n
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.select_object_content
    record_delim = args.record_delim
    if record_delim is not None:
        try:
            # Handle escape sequences.
            record_delim = record_delim.encode('utf-8').decode(
                'unicode_escape')
            input_serial['CSV']['RecordDelimiter'] = record_delim
        except Exception as e:
            raise ValueError(
                f"Record delim must utf-8 string or escape sequence. Err: {e}")

    field_delim = args.field_delim
    if field_delim is not None:
        try:
            # Handle escape sequences.
            field_delim = field_delim.encode('utf-8').decode('unicode_escape')
            input_serial['CSV']['FieldDelimiter'] = field_delim
        except Exception as e:
            raise ValueError(
                f"Field delim must be utf-8 string or escape sequence. Err: {e}"
            )

    quote_char = args.quote_char
    if quote_char is not None:
        try:
            # Handle escape sequences.
            quote_char = quote_char.encode('utf-8').decode('unicode_escape')
            if len(quote_char) != 1:
                raise ValueError("Quote char must be single "
                                 f"character, not: {quote_char}")
            input_serial['CSV']['QuoteCharacter'] = quote_char
        except Exception as e:
            raise ValueError(f"Quote character must be single utf-8 "
                             f"char or escape sequence. Err: {e}")

    quote_escape_char = args.quote_escape_char
    if quote_escape_char is not None:
        try:
            # Handle escape sequences.
            quote_escape_char = quote_escape_char.encode('utf-8').decode(
                'unicode_escape')
            if len(quote_escape_char) != 1:
                raise ValueError("Quote escape char must be single "
                                 f"character, not: {quote_escape_char}")
            input_serial['CSV']['QuoteEscapeCharacter'] = quote_escape_char
        except Exception as e:
            raise ValueError(f"Quote escape character must be single utf-8 "
                             f"char or escape sequence. Err: {e}")

    allow_quoted_rec_delim = args.allow_quoted_record_delimiter
    if allow_quoted_rec_delim not in ["true", "false"]:
        err(schema_parser.format_help())
        raise ValueError(
            f"Allow quoted record delimiter must be in [true, false]")
    if allow_quoted_rec_delim == "true":
        input_serial['CSV']['AllowQuotedRecordDelimiter'] = True
    else:
        input_serial['CSV']['AllowQuotedRecordDelimiter'] = False

    discovery_args["input_serial"] = input_serial

    # Just to be sure
    for key, val in discovery_args.items():
        if val is None:
            raise ValueError(f"Problem finding argument: {key}")

    return discovery_args


def err(msg):
    sys.stderr.write(f"{msg}\n")
    sys.stderr.flush()


def get_arg_parsers():
    parser = argparse.ArgumentParser(
        description=("Bulk find schemas of CSV files and load"))

    subparsers = parser.add_subparsers(
        dest="command", help="Subcommand to run")

    #
    # Schema Command
    #
    schema_parser = subparsers.add_parser(
        "schema",
        help=("Discover Schemas. Recursively search in SOURCE_PATH for"
              "CSV files and generate metadata for loading"))
    schema_req_args = schema_parser.add_argument_group("required named args")
    schema_req_args.add_argument(
        "-t",
        "--target_name",
        help=(
            "Xcalar Target Name for S3 Connector. Requires read/write perms. "
            "Default: 'Xcalar S3 Connector'"),
        default="Xcalar S3 Connector",
        dest="target_name")
    schema_req_args.add_argument(
        "-m",
        "--metadata_path",
        help=("S3 path to write the metadata files into. "
              "Example: /bucket/path/to/output/metadata/"),
        dest="metadata_path")
    schema_req_args.add_argument(
        "-s",
        "--src_path",
        help=("S3 path to source directory to scan for CSV files. "
              "Example: /bucket/path/to/files/"),
        dest="source_path")
    schema_req_args.add_argument(
        "-p",
        "--name_pattern",
        help=("File name pattern to match. Default: '*.csv'"),
        default='*.csv',
        dest="name_pattern")
    schema_req_args.add_argument(
        "-r",
        "--recursive",
        help=("Whether to decend into directories or not"),
        default="true",
        choices=["true", "false"],
        dest="recursive")
    schema_req_args.add_argument(
        "-n",
        "--max_num_files",
        help=
        ("Max number of files to scan. Default: 1. Use validate parsing args."
         ),
        default=1,
        dest="max_num_files")
    schema_parser.add_argument(
        "-x",
        "--num_threads",
        help=("Number of threads to run. Default: 16"),
        default=16,
        dest="num_threads")

    # CSV parsing options
    schema_req_args.add_argument(
        "--file_header",
        help=("Use first row as header?"),
        choices=["USE", "IGNORE", "NONE"],
        default="USE",
        dest="file_header_info")

    schema_parser.add_argument(
        "--record_delim",
        help=("Character to use for record delimiter."),
        dest="record_delim")

    schema_parser.add_argument(
        "--field_delim",
        help=("Character to use for field delimiter."),
        dest="field_delim")

    schema_parser.add_argument(
        "--quote_char",
        help=("Character to use for quoting."),
        dest="quote_char")

    schema_parser.add_argument(
        "--quote_escape_char",
        help=("Character to use for quote escaping."),
        dest="quote_escape_char")

    schema_parser.add_argument(
        "--allow_quoted_record_delimiter",
        help=("Allow quoted record delimiter?"),
        choices=["true", "false"],
        default="false",
        dest="allow_quoted_record_delimiter")

    #
    # Load Command
    #

    load_parser = subparsers.add_parser("load", help="load files from schema")
    load_req_args = load_parser.add_argument_group("required named args")
    load_parser.add_argument(
        "-x",
        "--num_threads",
        help=("Number of threads to run. Default: 8. "
              "Note: should not exceed num cores in cluster."),
        default=8,
        dest="num_threads")

    load_req_args.add_argument(
        "-t",
        "--target_name",
        help=(
            "Xcalar Target Name for S3 Connector. Requires read perms. "
            "Default: 'Xcalar S3 Connector'. Note: Must be same target used in "
            f"'{parser.prog} schema' command"),
        default="Xcalar S3 Connector",
        dest="target_name")

    load_req_args.add_argument(
        "-m",
        "--metadata_path",
        help=("S3 path to metadata directory generated by "
              f"'{parser.prog} schema ...' command. Example: "
              "/bucket/path/to/output/metadata/"),
        dest="metadata_path")

    load_req_args.add_argument(
        "-f",
        "--allow_failures",
        help=("Allow table load failures. Script will continue "
              "to attempt to load tables even if one fails. "
              "Default is: false"),
        default="false",
        choices=["true", "false"],
        dest="allow_failures")

    load_parser.add_argument(
        "-z",
        "--stop_on_table_error",
        help=("Stop if there are error table when loading data. Default=true"),
        choices=["true", "false"],
        default="true",
        dest="stop_on_table_error")
    return (parser, schema_parser, load_parser)


if __name__ == "__main__":

    (parser, schema_parser, load_parser) = get_arg_parsers()

    # Use this for the help option.
    def print_help():
        err(parser.format_help())
        subparsers_actions = [
            action for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        ]
        for subparsers_action in subparsers_actions:
            for choice, subparser in subparsers_action.choices.items():
                err(f"\n{choice} command:")
                err(subparser.format_help())

    parser_args = parser.parse_args()
    command = parser_args.command
    if command is None:
        print_help()
        err("No command was specified")
    elif command == 'schema':
        discovery_args = None
        try:
            discovery_args = build_args_for_discovery(parser_args)
        except Exception as e:
            err(schema_parser.format_help())
            err(f"Invalid argument: {e}")
        else:
            discover_concurrent(
                discovery_args["target_name"], discovery_args["source_path"],
                discovery_args["name_pattern"], discovery_args["recursive"],
                discovery_args["metadata_path"],
                discovery_args["input_serial"],
                discovery_args["max_num_files"], discovery_args["num_threads"])

    elif command == 'load':
        load_args = None
        try:
            load_args = build_args_for_load(parser_args)
        except Exception as e:
            err(load_parser.format_help())
            err(f"Invalid argument: {e}")
        else:
            load_concurrent(
                load_args["target_name"], load_args["metadata_path"],
                load_args["num_threads"], load_args["stop_on_table_error"],
                load_args["allow_failures"])
