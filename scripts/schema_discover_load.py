import os
import re
import json
import traceback

from datetime import datetime

import xcalar.container.context as ctx
import xcalar.container.xpu_host as xpu_host
from xcalar.container.cluster import get_running_cluster
from xcalar.external.client import Client
from xcalar.container.target.manage import get_target_data, build_target

from xcalar.container.util import cleanse_column_name

from xcalar.container.schema_discover.schema_discover_dataflows import get_load_table_query_string
from xcalar.container.schema_discover.aws_parser import add_compression_type

from xcalar.container.loader.load_wizard_load_plan import (
    LoadWizardLoadPlanBuilder)

from botocore.exceptions import ClientError

app_name = "SchemaLoad"

DEFAULT_LOAD_WIZARD_TARGET_NAME = "Xcalar S3 Connector"

FILE_TYPE_CSV = "CSV"
FILE_TYPE_JSONL = "JSONL"
FILE_TYPE_JSONDOC = "JSON"
FILE_TYPE_PARQUET = "PARQUET"

DF_UNKNOWN = "DfUnknown"
DF_INT = "DfInt64"
DF_BOOL = "DfBoolean"
DF_FLOAT = "DfFloat64"
DF_STRING = "DfString"
DF_ARRAY = "DfArray"

# Should put these in a util.py somewhere.
int_regex = re.compile(r'^(-|\+)?[0-9]+$')
float_regex = re.compile(r'^(-|\+)?([0-9]+\.([0-9]+)?|\.[0-9]+)$')

# https://github.com/apache/arrow/blob/6c319407620f98481c3e38fc83eaafcc2acff4aa/cpp/src/parquet/file_reader.cc#L49
# This is just the default read size.
# PARQUET_FOOTER_SIZE = 64 * 1024
PARQUET_FOOTER_SIZE = 3 * 2**20    # ENG-10046, fix next sprint -> ENG-10049
XCE_SUCCESS_MESSAGE = 'XCE-00000000 Success'

logger = ctx.get_logger()

#
# XXX This file is getting unruly ... think about moving
# some of this to different files or changing structure.
#


class InvalidApiInputError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class TopLevelArrayError(Exception):
    def __init__(self):
        self.message = ("This JSON file appears to start with an array. "
                        "Only JSONL is currently supported.")
        super().__init__(self.message)


def dumb_type_guess(obj):
    if obj is None:
        # Is this correct? Not sure when we have a None?
        # Review the following bug in greater detail later:
        # ENG-9556
        #
        # The cast ops will throw TypeErrors
        # if we don't handle none types. Hrm.
        return DF_STRING

    # Booleans must be checked first
    if isinstance(obj, bool):
        return DF_BOOL
    if isinstance(obj, int):
        return DF_INT
    if isinstance(obj, float):
        return DF_FLOAT
    if isinstance(obj, list):
        # This gets handled somewhere else.
        return DF_ARRAY
    if isinstance(obj, str):
        if obj.lower() in ["false", "true"]:
            return DF_BOOL
        if re.match(int_regex, obj):
            return DF_INT
        if re.match(float_regex, obj):
            return DF_FLOAT

    return DF_STRING


def escape_key_for_mapping(key):
    # In order to use S3 Select, we need to use a JSONPath
    # syntax, which Kinesis called "mapping" - maybe this
    # is a poor name. Parquet calls this "path" ... anyway...
    #
    # When there is a double quote in the key, we need to
    # escape it by double quoting it. E.g.,
    #   key(foo"bar) -> mapping("foo""bar")
    #
    # This also allows for spaces in the key name:
    #   key(foo bar) -> mapping("foo bar")
    #
    # There may be more conditions we find later.
    escaped_key = ''.join([f'"{cc}' if cc == '"' else cc for cc in str(key)])
    return f'"{escaped_key}"'


def get_schema_csv(row_tuple_list):
    new_row = {}
    status = {
        "unsupported_columns": [],
        "error_message": None    # Is this needed anymore?
    }
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
            # If there are trailing columns, we will have blank keys returned
            # from AWS if a header is used.
            status["unsupported_columns"].append({
                "name": col_name,
                "mapping": col_mapping,
                "message": f"Trailing columns detected with value({val})"
            })
            continue
        columns.append({
            "name": col_name,
            "mapping": col_mapping,
            "type": dumb_type_guess(val)
        })
        new_row[key] = val

    # Make the row look like the original but without trailing cols
    # Need it to be formatted like AWS formats JSON.
    new_row_json = json.dumps(new_row, separators=(',', ':')) + '\n'
    return new_row_json, columns, status


# XXX This naming conflicts with array names now. However, name
# conflicts are not critical at this point because we allow the
# user to rename the columns.
def check_for_duplicate_column_names(schema):
    # XXX Think about this more later. This algo look poor.
    col_names = [cc["name"] for cc in schema["columns"]]
    col_counts = {kk: 0 for kk in set(col_names)}
    for col_name in col_names:
        col_counts[col_name] += 1

    col_dup_names = [name for name, count in col_counts.items() if count > 1]

    # Python dicts preserve insert order, so this is deterministic.
    for col in schema["columns"]:
        try:
            col_name_idx = col_dup_names.index(col["name"])
            cur_count = col_counts[col["name"]]
            tot_count = col_names.count(col["name"])
            col["name"] = f'{col["name"]}_{tot_count - cur_count}'
            col_counts[col_dup_names[col_name_idx]] -= 1
        except ValueError:
            pass
    # So, given these columns: [X, X, X_0, X_1], this would
    # still produce a duplicate, but it is fine for now..


def check_for_top_level_array(schema):
    # If all mappings start with this, source data
    # contained only an array. I.e., probably JSON
    tla_prefixed = [
        col['mapping'].startswith('$."_1"[') for col in schema['columns']
    ]
    if all(tla_prefixed):
        raise TopLevelArrayError()


def get_input_file_type(input_serial):
    if "JSON" in input_serial:
        if input_serial["JSON"]["Type"] == "DOCUMENT":
            return FILE_TYPE_JSONDOC
        elif input_serial["JSON"]["Type"] == "LINES":
            return FILE_TYPE_JSONL
    if "CSV" in input_serial:
        return FILE_TYPE_CSV
    if "Parquet" in input_serial:
        return FILE_TYPE_PARQUET


def validate_source_args(source_args):
    required_keys = ["path", "fileNamePattern", "recursive", "targetName"]
    if not isinstance(source_args, list):
        raise ValueError("sourceArgs must be a list")
    for ii, source in enumerate(source_args):
        for key in required_keys:
            try:
                source[key]
            except KeyError:
                raise ValueError(f"sourceArg[{ii}] missing contain '{key}'")
    return


# XXX Need tests for user manipulating the Path Mapping in strange ways.
mapping_array_regex = re.compile(r'\[[^\d+]\]')


def validate_mapping(mapping):
    if not mapping.startswith('$'):
        raise ValueError("mapping must start with '$'")
    miter = re.finditer(mapping_array_regex, mapping)
    if miter is not None:
        for m in miter:
            array_index = m.group(1)
            raise ValueError("Array index must only contain digits")


# This allows duplicate keys where a dict does not
# E.g., '{"A": 1, "A": 2}' -> [('A', 1), ('A', 2)]
def json_loads_tuple_list(json_str):
    def obj_pairs_hook(obj):
        return obj

    row_tuple_list = json.loads(json_str, object_pairs_hook=obj_pairs_hook)
    return row_tuple_list


# XXX
# XXX For CSV, we cannot select extra columns, we need to add
# XXX these columns afterwards in the parser.
# XXX


def validate_schema(schema):
    MAX_NUM_COLUMNS = 1000
    required_schema_keys = ["name", "mapping", "type"]
    allowed_types = ["DfBoolean", "DfFloat64", "DfInt64", "DfString"]
    if not isinstance(schema, dict):
        raise ValueError("schema must be a dict")

    rowpath = schema.get("rowpath")
    if not rowpath:
        raise ValueError("schema must contain 'rowpath' key")
    if not rowpath.startswith('$'):
        raise ValueError("rowpath must start with '$'")
    columns = schema.get("columns")
    if not columns:
        raise ValueError("schema must contain 'columns' key")
    if not isinstance(columns, list):
        raise ValueError("schema['columns'] must be list")
    if len(columns) > MAX_NUM_COLUMNS:
        raise ValueError(f"Columns({len(columns)}) must not exceed 1000")

    for ii, source in enumerate(columns):
        for key in required_schema_keys:
            val = source.get(key)
            if not val:
                raise ValueError(
                    f"schema['columns'][{ii}] missing contain '{key}'")
            if key == "mapping":
                validate_mapping(val)


def validate_input_serial(input_serial):
    if not isinstance(input_serial, dict):
        raise ValueError("'input_serial' must be a dict")
    # XXX
    # XXX Write this later...
    # XXX
    return


class SchemaDiscover():
    def __init__(self, session_name, user_name):
        client = Client(bypass_proxy=True, user_name=user_name)
        session = None
        try:
            session = client.get_session(session_name)
        except Exception:
            session = client.create_session(session_name)

        self._client = client
        self._session = session
        self._target = None

        # XXX This might make more sense as a global which could
        # be included from ouside of this function from another file.
        # Other callers, LoadMart? might want to use this regex
        self.load_id_regex_str = r'(LOAD_WIZARD_[0-9A-F]{16}_\d{10}_\d{6})_'
        self.load_id_regex = re.compile(self.load_id_regex_str)

        return

    def upload_udf(self, module_name, udf_source):
        self._client.create_udf_module(module_name, udf_source)

    def _get_target(self, target_name, path):
        target_data = get_target_data(target_name)
        if self._target is None:
            target = build_target(
                target_name,
                target_data,
                path,
                num_nodes=ctx.get_node_count(),
                node_id=ctx.get_node_id(ctx.get_xpu_id()),
                user_name=ctx.get_user_id_name(),
                num_xpus_per_node=ctx.get_num_xpus_per_node())
            self._target = target
        return self._target

    ##
    # Functions for generating Preview response:

    def get_row_gen(self,
                    target_name,
                    path,
                    input_serial,
                    num_rows,
                    output_serial=None):
        target = self._get_target(target_name, path)
        query = f'SELECT * FROM s3Object s LIMIT {num_rows}'
        with target:
            # XXX
            # XXX FIXME Handle errors...
            # XXX
            add_compression_type(input_serial, path)
            with target.open(path, 's3select') as s3select:
                row_gen = s3select.rows(input_serial, query)
                return row_gen

    def get_jsonl_preview_response(self, target_name, path, input_serial,
                                   num_rows):
        rows = []
        schemas = []
        statuses = []
        row_gen = self.get_row_gen(target_name, path, input_serial, num_rows)
        for row in row_gen:
            schema = {"columns": [], "rowpath": ""}
            status = {"unsupported_columns": [], "error_message": None}
            file_type = get_input_file_type(input_serial)
            try:
                schema_json = xpu_host.find_schema("JSON", row.encode('utf-8'))
                schema = json.loads(schema_json)
                check_for_top_level_array(schema)
                check_for_duplicate_column_names(schema)
                # This pattern will go away after I fix the statuses structure.
                row_status = schema.pop('status', XCE_SUCCESS_MESSAGE)
                if row_status == XCE_SUCCESS_MESSAGE:
                    status["error_message"] = None
                else:
                    # If columns exceed 1000, we will put message here.
                    status["error_message"] = row_status
            except Exception as e:
                status["error_message"] = str(e)

            rows.append(row)
            schemas.append(schema)
            statuses.append(status)
        preview_response = {
            "rows": rows,
            "schemas": schemas,
            "statuses": statuses
        }
        return preview_response

    def get_csv_preview_response(self, target_name, path, input_serial,
                                 num_rows):
        # So, if we hit a trailing header during schema detection, we need to make
        # a note of that in the "statuses" field and suggest using Header NONE

        rows = []
        schemas = []
        statuses = []
        row_gen = self.get_row_gen(target_name, path, input_serial, num_rows)

        for row in row_gen:
            row_tuple_list = json_loads_tuple_list(row)
            start_path = '$'

            new_row, columns, status = get_schema_csv(row_tuple_list)
            schema = {"rowpath": start_path, "columns": columns}

            # XXX Is this needed? Oh, yeah, column names.
            check_for_duplicate_column_names(schema)

            rows.append(new_row)
            schemas.append(schema)
            statuses.append(status)
        preview_response = {
            "rows": rows,
            "schemas": schemas,
            "statuses": statuses
        }
        return preview_response

    # So, for parquet, we want to return one schema, and then the number of
    # rows requested. There isn't any reason to return more.
    def get_parquet_preview_response(self, target_name, path, input_serial,
                                     num_rows):
        # Read metadata from Parquet file
        schema = {"columns": [], "rowpath": ""}
        status = {"unsupported_columns": [], "error_message": None}
        target = self._get_target(target_name, path)
        rows = []
        with target.open(path, 'rb') as file_obj:
            file_size = file_obj.seek(0, os.SEEK_END)
            pos = 0
            if file_size > PARQUET_FOOTER_SIZE:
                pos = file_size - PARQUET_FOOTER_SIZE
            file_obj.seek(pos, os.SEEK_SET)
            footer_bytes = file_obj.read(PARQUET_FOOTER_SIZE)
            try:
                schema_json = xpu_host.find_schema("Parquet", footer_bytes)
                schema = json.loads(schema_json)
                check_for_duplicate_column_names(schema)
                # This pattern will go away after I fix the statuses structure.
                row_status = schema.pop('status', XCE_SUCCESS_MESSAGE)
                if row_status == XCE_SUCCESS_MESSAGE:
                    status["error_message"] = None
                else:
                    # If columns exceed 1000, we will put message here.
                    status["error_message"] = row_status
            except Exception as e:
                status["error_message"] = str(e)

        # XXX TODO: Move this into the try loop, shouldn't return data on error.
        row_gen = self.get_row_gen(target_name, path, input_serial, num_rows)
        rows = [row for row in row_gen]

        preview_response = {
            "rows": rows,
            "schemas": [schema],    # There can be only one! :)
            "statuses": [status]
        }

        return preview_response

    ##
    # Functions for generating query strings
    # XXX I think some of these can be consolidated.

    def gen_load_query_strings(self, source_args, load_plan_full_name, schema,
                               load_table_name):

        parser_fn_name = load_plan_full_name
        parser_arg_json = json.dumps({
            'loader_name': 'LoadWizardLoader',    # XXX Should be parameterized
            'loader_start_func': 'load_with_load_plan'
        })
        (query_string, optimized_query_string) = get_load_table_query_string(
            source_args, parser_fn_name, parser_arg_json, schema,
            load_table_name)

        logger.info(
            f"Generated 'load' query string for table ({load_table_name})")

        return (query_string, optimized_query_string)


def main(in_blob):
    try:
        in_obj = json.loads(in_blob)    # XXX handle decode error

        cluster = get_running_cluster()

        # AppLoader.cpp only grabs the first non-empty return
        # XXX Should probably look at AppLoader.cpp more closely.
        if cluster.my_xpu_id != cluster.local_master_for_node(0):
            return ""

        session_name = in_obj.get('session_name', None)
        if session_name is None:
            raise InvalidApiInputError(
                "Key 'session_name' not found in request")

        user_name = in_obj.get('user_name', None)
        schema_discover = SchemaDiscover(session_name, user_name)

        func = in_obj.get('func', None)
        if func is None:
            raise InvalidApiInputError("Key 'func' was not specified")

        elif func == 'get_load_id':
            # I do not think we should use microseconds as a unique thing.
            load_id_printf = 'LOAD_WIZARD_{session_id}_{secs}_{microsecs}'
            datetime_now = datetime.now()
            datetime_epoch = datetime.utcfromtimestamp(0)
            time_delta = (datetime_now - datetime_epoch)
            seconds_since_epoch = time_delta.total_seconds()

            epoch_micro_parts = str(seconds_since_epoch).split('.')
            seconds = epoch_micro_parts[0].ljust(10, '0')
            microsecs = epoch_micro_parts[1].ljust(6, '0')

            session_id = schema_discover._session._session_id
            load_id = load_id_printf.format(
                session_id=session_id, secs=seconds, microsecs=microsecs)
            logger.info(f"get_load_id(), return({load_id})")

            return json.dumps(load_id)

        elif func == 'preview_rows':
            try:
                preview_response = {"rows": [], "schemas": [], "statuses": []}
                global_status = {
                    "error_message": None,
                }

                load_id = None
                path = None
                target_name = None
                input_serial = None
                num_rows = None

                try:
                    path = in_obj['path']
                    target_name = in_obj['target_name']
                    input_serial_json = in_obj['input_serial_json']
                    num_rows = in_obj['num_rows']
                    load_id = in_obj['load_id']

                    input_serial = json.loads(input_serial_json)

                    # XXX FIXME Need to sample ranges.
                    if num_rows > 20:
                        raise InvalidApiInputError(
                            f"Sample size greater than 20 is not supported")

                except KeyError as e:
                    raise InvalidApiInputError(
                        f"Schema Load request missing key({e.args[0]})")
                except json.JSONDecodeError as e:
                    raise InvalidApiInputError(
                        f"Invalid 'input_serial_json' sepecified: {e}")

                file_type = get_input_file_type(input_serial)
                if not file_type:
                    raise InvalidApiInputError(
                        f"Could not determine file_type from provide input serialization"
                    )

                if file_type == FILE_TYPE_JSONDOC:
                    raise InvalidApiInputError(
                        f"JSON Document is not supported in this release")

                logger.info(
                    f'Preview Rows: path="{path}", input_serial="{input_serial_json}"'
                )

                # XXX Should change "statuses" from:
                # [ {"unsupported_columns": [], "error_message": ""}, ..]
                # ... to ...
                # [ "status message for row 1", "status message row 2", ... ]
                # ... because there are no longer unsupported columns
                # ... and the message is a status, not an error.

                if file_type == FILE_TYPE_JSONL:
                    preview_response = schema_discover.get_jsonl_preview_response(
                        target_name, path, input_serial, num_rows)

                if file_type == FILE_TYPE_PARQUET:
                    preview_response = schema_discover.get_parquet_preview_response(
                        target_name, path, input_serial, num_rows)

                if file_type == FILE_TYPE_CSV:
                    preview_response = schema_discover.get_csv_preview_response(
                        target_name, path, input_serial, num_rows)

            except ClientError as e:
                # XXX Test this
                # These are AWS Errors that should be reported to the user.
                global_status[
                    'error_message'] = f"AWS ERROR: {e.response['Error']['Message']}"
                logger.error(
                    f'Preview Rows: AWS ERROR: path="{path}", input_serial="{input_serial_json}", '
                    f'error="{e.response["Error"]["Message"]}"')
            except TopLevelArrayError as e:
                # Test this
                global_status['error_message'] = f"DATA ERROR: {e.message}"
            except InvalidApiInputError as e:
                # XXX Test this
                global_status['error_message'] = f"INPUT ERROR: {e.message}"
            except Exception as e:
                # A truely unexpected error, pass stack forward for debugging.
                global_status["error_message"] = "Internal Error"
                global_status[
                    "exception"] = f"SchemaLoad Error({e}): {traceback.format_exc()}"
                logger.error(
                    f'PreviewRows: Interal Error: path="{path}", load_id="{load_id}", '
                    f'input_serial="{input_serial_json}", error="{e}", '
                    f'stack="{traceback.format_exc()}"')
            finally:
                preview_response["global_status"] = global_status
                logger.info(
                    f'PreviewRows: Finished: path="{path}", '
                    f'input_serial="{input_serial_json}", '
                    f'load_id="{load_id}", '
                    f'global_error_message="{global_status["error_message"]}"')
            return json.dumps(preview_response)

        elif func == 'get_dataflows_with_schema':
            # XXX Should probably put a return in the finally block
            try:
                source_args_json = in_obj['source_args_json']
                schema_json = in_obj['schema_json']
                input_serial_json = in_obj['input_serial_json']

                load_table_name = in_obj['load_table_name']

                # Fail API early if jsons are invalid
                input_serial = json.loads(input_serial_json)
                source_args = json.loads(source_args_json)
                schema = json.loads(schema_json)
            except KeyError as e:
                raise InvalidApiInputError(
                    f"Schema Load request missing key({e.args[0]})")
            except json.JSONDecodeError as e:
                # XXX Validate this
                raise InvalidApiInputError(
                    f"Invalid JSON sepecified for: {e.args[0]}: {e}")

            num_rows = in_obj.get('num_rows')    # XXX Used for sample.
            if num_rows is not None:
                try:
                    num_rows = int(num_rows)
                except ValueError as e:
                    raise ValueError(
                        f"Problem with num_rows value({num_rows}): {e}")

            # q_LOAD_WIZARD_5F5014FC31A88B57_1599058595_251320_(load|data|comp)
            # xl_LOAD_WIZARD_5F5014FC31A88B57_1599058595_251320_(load|data|comp)
            load_id = None
            m = re.search(schema_discover.load_id_regex, load_table_name)
            if m is None:
                raise ValueError(f"load_table_name does not match ("
                                 f"{schema_discover.load_id_regex_str})."
                                 f"Please use get_load_id() function")
            load_id = m.group(1)
            logger.info(f"{app_name}: load_id={load_id}")
            if load_id is None:
                raise ValueError(f"load_id was not matched: {m}")

            # Raises ValueErrors:
            validate_source_args(source_args)
            validate_schema(schema)
            validate_input_serial(input_serial)

            target_name = in_obj.get('target_name',
                                     DEFAULT_LOAD_WIZARD_TARGET_NAME)

            # Make Load Plan
            # XXX Since there isn't a big file list anymore,
            # we could move this into the KVStore.
            module_name = f"LOAD_PLAN_UDF_{load_id}"
            load_plan_builder = LoadWizardLoadPlanBuilder()
            load_plan_builder.set_schema_json(json.dumps(schema))
            load_plan_builder.set_load_id(load_id)
            load_plan_builder.set_input_serial_json(input_serial_json)
            if num_rows is not None:
                load_plan_builder.set_num_rows(num_rows)

            (udf_source, func_name) = load_plan_builder.get_load_plan()
            load_plan_full_name = f"{module_name}:{func_name}"

            # Upload Load Plan
            schema_discover.upload_udf(module_name, udf_source)

            # Generate Load dataflow
            (load_qs, load_opt_qs) = schema_discover.gen_load_query_strings(
                source_args, load_plan_full_name, schema['columns'],
                load_table_name)

            query_strings = json.dumps({
                'load_df_query_string': load_qs,
                'load_df_optimized_query_string': load_opt_qs,
            })
            return query_strings

        else:
            raise ValueError("Unknown function")

    except Exception as e:
        logger.error(f"SchemaLoad Error({e}): {traceback.format_exc()}")
        raise
