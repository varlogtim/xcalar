from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.compute.util.upload_stats_util import table_load_dataflow
from xcalar.compute.util.utils import tar_cz
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
import argparse
import json
import os


port = os.getenv("XCE_HTTPS_PORT", '8443')
parser = argparse.ArgumentParser()
parser.add_argument("-l",
                    "--logs_path",
                    required=False,
                    help="Path to where the logs are located or defaults to XcalarLogCompletePath of the cluster connected",
                    default="")
parser.add_argument("-x",
                    "--xcalar_host",
                    required=False,
                    default="localhost:{}".format(port),
                    type=str,
                    help='Xcalar cluster hostname and port (as hostname:port)')
parser.add_argument("-u",
                    "--xcalar_user",
                    required=False,
                    help="username of xcalar",
                    default="admin")
parser.add_argument("-p",
                    "--xcalar_pass",
                    required=False,
                    help="password of xcalar",
                    default="admin")

args = parser.parse_args()
url = 'https://{}'.format(args.xcalar_host)
username = args.xcalar_user
password = args.xcalar_pass

schema_xpu = {
    'TIMESTAMP': 'TIMESTAMP',
    'TARGET_NAME': 'STRING',
    'FILE_PATH': 'STRING',
    'FILE_SIZE': 'INT',
    'XPU_ID': 'INT',
    'LOAD_ID': 'STRING',
    'FILE_TYPE': 'STRING',
    'NUM_OF_COLS': 'INT'
}

schema_node = {
    'TIMESTAMP': 'TIMESTAMP',
    'JOB_ID': 'STRING',
    'TABLE_NAME': 'STRING',
    'LOAD_ID': 'STRING',
    'TABLE_TYPE': 'STRING',
    'TABLE_ROW_COUNT': 'INT',
    'TABLE_EXECUTION_TIME': 'FLOAT',
}

schema_preview = {
    'TIMESTAMP': 'TIMESTAMP',
    'FILE_PATH': 'STRING',
    'INPUT_SERIAL': 'STRING',
    'LOAD_ID': 'STRING',
    'ERR_MSG': 'STRING'
}

map_cols_table = {
    "STRING": "DfString",
    "INTEGER": "DfInt64",
    "BOOLEAN": "DfBoolean",
    "FLOAT": "DfFloat64",
    "MONEY": "DfMoney",
    "INT": "DfInt64",
    "TIMESTAMP": "DfTimespec"
}

client = Client(url=url, client_secrets={"xiusername": username, "xipassword": password})
xcalarApi = XcalarApi(url=url, client_secrets={"xiusername": username, "xipassword": password})
logs_path = args.logs_path
if not logs_path:
    logs_path = client.get_config_params(param_names=['XcalarLogCompletePath'])[0]['param_value']
    print("Defaulting logs_path to XcalarLogCompletePath")

xpu_log_path = os.path.join(logs_path, 'xpu.out')
node_log_path = os.path.join(logs_path, 'node.0.log')

if not os.path.exists(xpu_log_path):
    raise Exception("xpu.out does not exist in the given path '{}'".format(logs_path))
if not os.path.exists(node_log_path):
    raise Exception("node.0.log does not exist in the given path '{}'".format(logs_path))

op = Operators(xcalarApi)
workbook_path = os.path.join(os.getenv("XLRDIR", "/opt/xcalar/"), "scripts/exampleWorkbooks/LoadMart/workbook")
try:
    workbook = client.get_workbook("LoadMart")
except Exception as e:
    workbook = client.upload_workbook("LoadMart", tar_cz(workbook_path))
session = workbook.activate()
xcalarApi.setSession(session)


def load_table(schema, path, parser_name, published_table_name, tmp_table_name):
    qs, retina_str = table_load_dataflow(schema, "Default Shared Root", path, parser_name, {})
    df = Dataflow.create_dataflow_from_query_string(client, qs, optimized_query_string=retina_str)
    try:
        session.execute_dataflow(df, table_name=tmp_table_name, optimized=True, is_async=False)
    except Exception as e:
        if "DAG name already exists" in str(e) or "Table already exists" in str(e):
            op.dropTable(tmp_table_name)
        else:
            raise
    try:
        op.unpublish(published_table_name)
    except Exception as e:
        pass
    op.publish(tmp_table_name, published_table_name)
    print("Published {}".format(published_table_name))
    op.dropTable(tmp_table_name)


for file in ['file', 'table', 'preview']:
    table_name = "Load_Stats_" + file
    tmp_table_name = 'tmp_' + table_name
    if 'file' in file:
        schema = schema_xpu
        parser_name = '/sharedUDFs/default:getDetailedFileStats'
        published_table_name = 'LM_FILE_TABLE'
        path = [xpu_log_path]
    elif 'table' in file:
        schema = schema_node
        parser_name = '/sharedUDFs/default:getDetailedLoadedTableStats'
        published_table_name = 'LM_LOADED_TABLES'
        path = [node_log_path]
    else:
        fileNamePattern = 'xpu.out'
        schema = schema_preview
        parser_name = '/sharedUDFs/default:getDetailedPreviewStats'
        published_table_name = 'LM_PREVIEW_TABLE'
        path = [xpu_log_path]
    load_table(schema, path, parser_name, published_table_name, tmp_table_name)
