import json
import uuid
import pprint

from xcalar.external.client import Client
from xcalar.external.dataflow import Dataflow
from xcalar.container.schema_discover.schema_discover_dataflows import (
    get_udf_load_query_string
)

script_prefix = 'opt_df_test'

client = Client(client_secrets={"xiusername": "admin", "xipassword": "admin"})
session_name = script_prefix
try:
    session = client.get_session(session_name)
except Exception:
    session = client.create_session(session_name)

udf_mod_name = f"{script_prefix}_mod_name"
udf_fn_name = f"{script_prefix}_fn_name"

UDF_SCHEMA = [
    {
        'name': 'A',
        'type': 'DfInt64'
    }, {
        'name': 'B',
        'type': 'DfBoolean'
    }
]
udf_src = """
def {udf_fn_name}(full_path, full_stream):
    for ii in range(10):
        yield dict(A=ii, B="false") #"b" + str(ii))
""".format(udf_fn_name=udf_fn_name)


def add_udf(udf_mod_name, udf_src):
    try:
        client.get_udf_module(udf_mod_name).delete()
    except Exception:
        pass
    client.create_udf_module(udf_mod_name, udf_src)


add_udf(udf_mod_name, udf_src)

print(f"Added udf: {udf_mod_name}:{udf_fn_name}")
print(f"Schema: {UDF_SCHEMA}")

uuid = str(uuid.uuid4())
table_name = f"{script_prefix}_table_{uuid}"


source_args = [{
    'targetName': 'Default Shared Root',
    'path': '/etc/hosts',
    'fileNamePattern': '',
    'recursive': False
}]
parser_fn_name = f"{udf_mod_name}:{udf_fn_name}"
parser_arg_json = json.dumps({})
(query, opt_query) = get_udf_load_query_string(
    source_args, parser_fn_name, parser_arg_json, UDF_SCHEMA, table_name)

print(f"Retrieved dataflows...")
print(f"Using table name: {table_name}")

dataflow = Dataflow.create_dataflow_from_query_string(
    client, query, optimized_query_string=opt_query)

pprint.pprint(json.loads(query), indent=4)

session.execute_dataflow(
    dataflow, table_name=table_name, optimized=True, is_async=False)

print(f"Executed dataflow")

table = session.get_table(table_name)

for rec in table.records():
    print(f"{rec}")
