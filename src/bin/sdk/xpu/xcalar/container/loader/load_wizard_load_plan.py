import json
import base64
import pickle
import logging

from collections import namedtuple
from xcalar.container.schema_discover.schema_discover_dataflows import (
    ICV_COLUMN_NAME,
    FRN_COLUMN_NAME,
    SRC_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
# This could be further abstracted by inheriting from a LoadPlan class

PICKLED_LOAD_PLAN_REPLACE = "#_XC_#PICKLED_LOAD_PLAN#_XC_#"
GET_PLAN_FUNCTION_NAME = "get_load_wizard_plan"
INTERNAL_COL_NAMES = [
    ICV_COLUMN_NAME, FRN_COLUMN_NAME, SRC_COLUMN_NAME, PATH_COLUMN_NAME
]

# XXX This assumes that the function is on the first line. Should address this in some way
# XXX Need to merge Manoj's changes for building the json_path stuff prior to the parser.

load_wizard_pickle_template = f"""
def {GET_PLAN_FUNCTION_NAME}():
    import base64
    import pickle
    pickled_load_plan='{PICKLED_LOAD_PLAN_REPLACE}'
    return pickle.loads(base64.b64decode(pickled_load_plan.encode('utf-8')))
"""

# XXX
# XXX Add a method which provides the needed parserArgs for this
# XXX
S3SelectCols = namedtuple("S3SelectCols", ['names', 'types'])

logger = logging.getLogger("xcalar")


class LoadWizardLoadPlan():
    def __init__(self,
                 file_list_json=None,
                 schema_json=None,
                 input_serial_json=None,
                 load_id=None,
                 num_rows=None):
        from xcalar.container.schema_discover.aws_parser import s3_select_parser
        self.s3_select_parser = s3_select_parser
        self.schema_json = schema_json
        self.input_serial_json = input_serial_json
        # XXX This is a hack to support easier logging.
        # Unique ID for session table and query names
        self.load_id = load_id
        self.file_type = None
        self.num_rows = num_rows

    def parser(self, full_path, in_stream):
        logger.info(f"LoadWizardLoadPlan.parser("
                    f"input_serial_json={self.input_serial_json}, "
                    f"cols={self.cols}, query='{self.query}'")
        yield from self.s3_select_parser(full_path, in_stream,
                                         self.input_serial_json, self.cols,
                                         self.query, self.file_type)

    def setup(self, query_col_printf=None):
        if query_col_printf is None:
            query_col_printf = 's.{mapping} as "{col_name}"'
        schema = json.loads(self.schema_json)

        rowpath = schema.get("rowpath")
        columns = schema.get("columns")
        if not rowpath:
            raise ValueError("Schema requires a 'rowpath' key")
        if not columns:
            raise ValueError("Schema requires a 'columns' key")

        TLA_ROWPATH = '$[0:]'

        top_level_array = rowpath == TLA_ROWPATH
        map_remove = TLA_ROWPATH if top_level_array else '$'
        col_aliases = []
        for col in columns:
            mapping = col['mapping'].replace(map_remove, '')
            mapping = '.'.join([f'{kk}' for kk in mapping.split('.') if kk])
            col_name = col['name']
            col_aliases.append(
                query_col_printf.format(mapping=mapping, col_name=col_name))

        aliases = ', '.join(col_aliases)
        obj_path = '[*][*]' if top_level_array else ''

        query_end = ''
        if self.num_rows is not None:
            # was cast to int() elsewhere
            query_end = f' LIMIT {self.num_rows}'

        self.query = f"SELECT {aliases} FROM s3Object{obj_path} s{query_end}"

        # XXX rethink this..
        self.cols = S3SelectCols(
            names=[
                c["name"].upper() for c in columns
                if c["name"] not in INTERNAL_COL_NAMES
            ],
            types=[c["type"] for c in columns])

        # Find "file type"
        input_serial = json.loads(self.input_serial_json)
        file_type = None
        if "JSON" in input_serial:
            if input_serial["JSON"]["Type"] == "DOCUMENT":
                file_type = "JSON"
            elif input_serial["JSON"]["Type"] == "LINES":
                file_type = "JSONL"
        if "CSV" in input_serial:
            file_type = "CSV"
        if "Parquet" in input_serial:
            file_type = "PARQUET"
        self.file_type = file_type


class LoadWizardLoadPlanBuilder:
    def __init__(self):
        # XXX Add class args and remove all these setters.
        # XXX Rename this class, actually, make it a function.
        # Perhaps a method on LoadWizardLoadPlan
        # XXX There must be some kind of Pythonic ways to make
        # setter/getter functions with constraints.
        self.load_plan_args = {
            "schema_json": None,
            "input_serial_json": None,
            "load_id": None,
            "num_rows": None
        }

        self.optinional_args = [
            "num_rows"
        ]

    def set_schema_json(self, schema_json):
        # Add validation here
        self.load_plan_args["schema_json"] = schema_json

    def set_input_serial_json(self, input_serial_json):
        # Check type and contents
        self.load_plan_args["input_serial_json"] = input_serial_json

    def set_load_id(self, load_id):
        self.load_plan_args["load_id"] = load_id

    def set_file_type(self, file_type):
        self.load_plan_args["file_type"] = file_type

    def set_num_rows(self, num_rows):
        if not isinstance(num_rows, int):
            raise ValueError(f"num_rows must be type int, not {type(num_rows)}")
        self.load_plan_args["num_rows"] = num_rows

    def get_load_plan(self):
        def key_unset(k):
            raise ValueError(f"set_{k} must be called first")

        [key_unset(k) for k, v in self.load_plan_args.items()
         if k not in self.optinional_args and v is None]

        # XXX rename these
        pickled_load_plan_args = pickle.dumps(self.load_plan_args)
        load_plan_p = base64.b64encode(pickled_load_plan_args).decode('utf-8')
        source = load_wizard_pickle_template.replace(PICKLED_LOAD_PLAN_REPLACE,
                                                     load_plan_p)
        return (source, GET_PLAN_FUNCTION_NAME)
