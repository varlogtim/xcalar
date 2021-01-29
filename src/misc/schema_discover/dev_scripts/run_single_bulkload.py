import re
import json
import uuid

from contextlib import contextmanager
from datetime import datetime

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.client import Client
from xcalar.container.loader.load_wizard_load_plan import (
    LoadWizardLoadPlanBuilder
)


LOAD_WIZARD_SESSION_NAME = "foo"
TARGET_NAME = "Xcalar S3 Connector"


@contextmanager
def setup_load_wizard(user_name=None):
    client = Client(bypass_proxy=True, user_name=user_name)
    xcalar_api = XcalarApi()
    workbook = client.create_workbook(LOAD_WIZARD_SESSION_NAME)
    xcalar_api.setSession(workbook)
    session = client.get_session(LOAD_WIZARD_SESSION_NAME)

    try:
        yield (client, session, workbook)
    finally:
        workbook.delete()
        xcalar_api.setSession(None)


@contextmanager
def dataset_context(workbook, source_args, parser_fn_name, parser_args):
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
    dataset.delete()


@contextmanager
def table_context(table):
    yield table
    print(f"Dropping table: {table.name}")
    table.drop(delete_completely=True)


@contextmanager
def loadplan_context(client, source_args, input_serial, schema, load_id):
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
    client.get_udf_module(mod_name).delete()


def get_load_id(session):
    # is fake.
    load_id_printf = 'LOAD_WIZARD_{session_id}_{secs}_{microsecs}'
    datetime_now = datetime.now()
    datetime_epoch = datetime.utcfromtimestamp(0)
    time_delta = (datetime_now - datetime_epoch)
    seconds_since_epoch = time_delta.total_seconds()

    epoch_micro_parts = str(seconds_since_epoch).split('.')
    seconds = epoch_micro_parts[0].ljust(10, '0')
    microsecs = epoch_micro_parts[1].ljust(6, '0')

    session_id = session._session_id
    load_id = load_id_printf.format(
        session_id=session_id, secs=seconds, microsecs=microsecs)
    return load_id


def do_load(source_args, input_serial, schema):
    print("Doing load...")
    with setup_load_wizard() as (client, session, workbook):
        load_id = get_load_id(session)
        with loadplan_context(
            client, source_args, input_serial, schema, load_id
        ) as (parser_fn_name, parser_args):
            with dataset_context(
                workbook, source_args, parser_fn_name, parser_args
            ) as dataset:
                print("done")


def get_func_name_from_loadplan(udf_src):
    func_name_regex = re.compile(f'def ([^(]+)')
    for mm in re.finditer(func_name_regex, udf_src):
        if not mm:
            continue
        return mm.group(1)
    raise RuntimeError("Could not find function name")


@contextmanager
def upload_loadplan(client, mod_name, udf_src):
    try:
        func_name = get_func_name_from_loadplan(udf_src)
        client.create_udf_module(mod_name, udf_src)

        load_plan_full_name = f"{mod_name}:{func_name}"

        parser_fn_name = load_plan_full_name
        parser_args = {
            'loader_name': 'LoadWizardLoader',
            'loader_start_func': 'load_with_load_plan'
        }
        yield (parser_args, parser_fn_name)
    finally:
        client.get_udf_module(mod_name).delete()

# Testing for bug ENG-10027


udf_src = """
def get_load_wizard_plan():
    import base64
    import pickle
    pickled_load_plan='gAN9cQAoWAsAAABzY2hlbWFfanNvbnEBWMUAAAB7InJvd3BhdGgiOiAiJCIsICJjb2x1bW5zIjogW3sibmFtZSI6ICJBIiwgIm1hcHBpbmciOiAiJC5cImFcIiIsICJ0eXBlIjogIkRmSW50NjQifSwgeyJuYW1lIjogIkIiLCAibWFwcGluZyI6ICIkLlwiYlwiIiwgInR5cGUiOiAiRGZJbnQ2NCJ9LCB7Im5hbWUiOiAiQyIsICJtYXBwaW5nIjogIiQuXCJjXCIiLCAidHlwZSI6ICJEZkludDY0In1dfXECWBEAAABpbnB1dF9zZXJpYWxfanNvbnEDWKEAAAB7IkNTViI6eyJGaWxlSGVhZGVySW5mbyI6IlVTRSIsIlF1b3RlRXNjYXBlQ2hhcmFjdGVyIjoiXCIiLCJSZWNvcmREZWxpbWl0ZXIiOiJcbiIsIkZpZWxkRGVsaW1pdGVyIjoiLCIsIlF1b3RlQ2hhcmFjdGVyIjoiXCIiLCJBbGxvd1F1b3RlZFJlY29yZERlbGltaXRlciI6ZmFsc2V9fXEEWAkAAABsb2FkX3V1aWRxBVgyAAAAYjVkMmE4MjFlZTc5ZWQyMmQ2OTQzMGI5ZWU3NmIwMmJfQjQ1MzZGMDk5M0M5NUE5OThxBlgJAAAAZmlsZV90eXBlcQdYAwAAAENTVnEIdS4='

    return pickle.loads(base64.b64decode(pickled_load_plan.encode('utf-8')))
"""
mod_name = "LOAD_WIZARD_b5d2a821ee79ed22d69430b9ee76b02b_B4536F0993C95A998"

# Manually modify.
source_args = {
    "path": "/xcfield/",
    "fileNamePattern": "a.csv",
    "recursive": False,
    "targetName": TARGET_NAME
}
input_serial = {"JSON": {"Type": "LINES"}}
schema = {
    "columns": [
        {"name": col_name.upper(), "mapping": f"$.{col_name}", "type": "DfInt64"}
        for col_name in ["a", "b", "c"]
        # {"name": f"C_{ii}", "mapping": f"$.C_{ii}", "type": "DfInt64"}
        # for ii in range(1, 21)
    ],
    "rowpath": "$"
}


def do_load_with_loadplan(source_args, input_serial, schema, mod_name, udf_src):
    with setup_load_wizard() as (client, session, workbook):
        load_id = get_load_id(session)
        with upload_loadplan(
                client, mod_name, udf_src
        ) as (parser_args, parser_fn_name):
            with dataset_context(
                workbook, source_args, parser_fn_name, parser_args
            ) as dataset:
                print("RECORDS:")
                for rec in dataset.records():
                    print(f"{rec}")


do_load_with_loadplan(source_args, input_serial, schema, mod_name, udf_src)


#do_load()
