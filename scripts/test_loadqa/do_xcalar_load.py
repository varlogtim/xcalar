# Copyright 2016-2020 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.

import json
import uuid
import hashlib
import logging
import sys
import time
from operator import add
from functools import reduce

from xcalar.external.dataflow import Dataflow
from xcalar.compute.services.SchemaLoad_xcrpc import SchemaLoad
from xcalar.compute.localtypes.SchemaLoad_pb2 import AppRequest


# Set up logging
logger = logging.getLogger('Load QA Suite')
logger.setLevel(logging.INFO)
if not logger.handlers:
    errorHandler = logging.StreamHandler(sys.stderr)
    formatterString = ('LOAD_QA: %(message)s')
    formatter = logging.Formatter(formatterString)
    errorHandler.setFormatter(formatter)
    logger.addHandler(errorHandler)


SESSION_NAME = str(uuid.uuid4())
LOAD = "load"
DATA = "data"
COMP = "comp"
PRIV_TARGET_NAME = "Xcalar S3 Connector"


def get_load_id(client, session):
    app_in = {
        "func": "get_load_id",
        "session_name": session.name,
    }
    app_in_json = json.dumps(app_in)
    app_out_raw = execute_schema_load_app(client, app_in_json)
    load_id = json.loads(app_out_raw.json)
    return load_id


def preview_source(client, target_name, path, input_serial_json, load_id, num_rows):
    """Construct a JSON for previewing schema

    Parameters:
    client (Client): the Xcalar Client
    target_name (str): name of the Xcalar target
    path (str): full path name of the s3 file to be previewed
    input_serial_json (str): input serialization for S3Select
    num_rows (int): number of rows to sample

    """

    app_in = {
        "func": "preview_rows",
        "session_name": SESSION_NAME,
        "target_name": target_name,
        "path": path,
        "input_serial_json": input_serial_json,
        "num_rows": num_rows,
        "load_id": load_id
    }
    app_in_json = json.dumps(app_in)
    resp = execute_schema_load_app(client, app_in_json)
    data = json.loads(resp.json)
    return data


def call_get_dataflows(
        client, session, source_args_json, input_serial_json, schema_json,
        load_table_name, data_table_name, comp_table_name, num_rows=None):

    app_in = {
        "func": "get_dataflows_with_schema",
        "session_name": session.name,
        "source_args_json": source_args_json,
        "input_serial_json": input_serial_json,
        "schema_json": schema_json,
        "load_table_name": load_table_name,
        "data_table_name": data_table_name,
        "comp_table_name": comp_table_name
    }
    if num_rows and isinstance(num_rows, int):
        app_in["num_rows"] = num_rows

    app_in_json = json.dumps(app_in)

    app_out_raw = execute_schema_load_app(client, app_in_json)
    app_output = json.loads(app_out_raw.json)
    (load_qs,
     load_qs_opt) = (app_output['load_df_query_string'],
                     app_output['load_df_optimized_query_string'])
    (data_qs,
     data_qs_opt) = (app_output['data_df_query_string'],
                     app_output['data_df_optimized_query_string'])
    (comp_qs,
     comp_qs_opt) = (app_output['comp_df_query_string'],
                     app_output['comp_df_optimized_query_string'])

    ret = {"load": (load_qs, load_qs_opt),
           "data": (data_qs, data_qs_opt),
           "comp": (comp_qs, comp_qs_opt)}

    return ret


def execute_schema_load_app(client, app_in_json):
    """Execute SchemaLoad app homed in scripts/schema_discover_load.py

    Parameters:
    client (Client): the Xcalar Client
    app_in_json (str): contains JSON app inputs

    """
    # raises xcalar.external.exceptions.XDPException
    schema_load = SchemaLoad(client)
    app_request = AppRequest(json=app_in_json)
    resp = schema_load.appRun(app_request)
    return resp


def call_get_dataflows(
        client, session, source_args_json, input_serial_json, schema_json,
        load_table_name, data_table_name, comp_table_name, num_rows=None):

    app_in = {
        "func": "get_dataflows_with_schema",
        "session_name": session.name,
        "source_args_json": source_args_json,
        "input_serial_json": input_serial_json,
        "schema_json": schema_json,
        "load_table_name": load_table_name,
        "data_table_name": data_table_name,
        "comp_table_name": comp_table_name
    }
    if num_rows and isinstance(num_rows, int):
        app_in["num_rows"] = num_rows

    app_in_json = json.dumps(app_in)

    app_out_raw = execute_schema_load_app(client, app_in_json)
    app_output = json.loads(app_out_raw.json)
    (load_qs,
     load_qs_opt) = (app_output['load_df_query_string'],
                     app_output['load_df_optimized_query_string'])
    (data_qs,
     data_qs_opt) = (app_output['data_df_query_string'],
                     app_output['data_df_optimized_query_string'])
    (comp_qs,
     comp_qs_opt) = (app_output['comp_df_query_string'],
                     app_output['comp_df_optimized_query_string'])

    ret = {"load": (load_qs, load_qs_opt),
           "data": (data_qs, data_qs_opt),
           "comp": (comp_qs, comp_qs_opt)}

    return ret


def do_load(client, session, source_args_json, input_serial_json,
            schema_json, load_id, num_rows=None, user_name=None):
    # returns handles to LOAD, COMP, and DATA tables
    data_tn = f"xl_{load_id}_data"
    comp_tn = f"xl_{load_id}_comp"
    load_tn = f"xl_{load_id}_load"
    data_qn = f"q_{load_id}_data"
    comp_qn = f"q_{load_id}_comp"
    load_qn = f"q_{load_id}_load"

    delay = {}

    start_time = time.time()
    dataflows = call_get_dataflows(
        client, session, source_args_json, input_serial_json, schema_json,
        load_tn, data_tn, comp_tn, num_rows=num_rows)
    stop_time = time.time()
    delay["get_dataflows"] = stop_time - start_time

    start_time = time.time()
    load_df = Dataflow.create_dataflow_from_query_string(
        client, dataflows['load'][0], optimized_query_string=dataflows['load'][1])
    stop_time = time.time()
    delay["create_load_dataflow"] = stop_time - start_time

    start_time = time.time()
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
    stop_time = time.time()
    delay["exec_load_dataflow"] = stop_time - start_time


    start_time = time.time()
    data_df = Dataflow.create_dataflow_from_query_string(
        client, dataflows['data'][0], optimized_query_string=dataflows['data'][1])
    stop_time = time.time()
    delay["create_data_dataflow"] = stop_time - start_time

    start_time = time.time()
    session.execute_dataflow(
        data_df,
        table_name=data_tn,
        query_name=data_qn,
        optimized=True,
        is_async=False)
    stop_time = time.time()
    delay["exec_data_dataflow"] = stop_time - start_time

    start_time = time.time()

    comp_df = Dataflow.create_dataflow_from_query_string(
        client, dataflows['comp'][0], optimized_query_string=dataflows['comp'][1])
    session.execute_dataflow(
        comp_df,
        table_name=comp_tn,
        query_name=comp_qn,
        optimized=True,
        is_async=False)
    stop_time = time.time()
    delay["exec_comp_dataflow"] = stop_time - start_time

    start_time = time.time()
    load_table = session.get_table(load_tn)
    data_table = session.get_table(data_tn)
    comp_table = session.get_table(comp_tn)
    stop_time = time.time()
    delay["get_tables"] = stop_time - start_time

    delay["total_load_delay"] = reduce(add, delay.values())
    delay["load_delay"] = delay["exec_comp_dataflow"] + delay["exec_data_dataflow"] + delay["exec_load_dataflow"]

    ret = {LOAD: load_table,
           DATA: data_table,
           COMP: comp_table}

    return ret, delay
