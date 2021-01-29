import os
import csv
import json
import uuid
import traceback
import xcalar.container.context as ctx
import xcalar.container.xpu_host as xpu_host
from xcalar.container.schema_discover.schema_discover_dataflows import (
    ICV_COLUMN_NAME, FRN_COLUMN_NAME, SRC_COLUMN_NAME, PATH_COLUMN_NAME)

logger = ctx.get_logger()

RECORD_DELIM = '\n'
FIELD_DELIM = ','
QUOTE_CHAR = '"'
QUOTE_ESCAPE_CHAR = "\\"
INTERNAL_CSV_DIALECT_NAME = 'AWS_S3_SELECT_DIALECT'

CSV_OUTPUT_SERIALIZATION = {
    "CSV": {
        "QuoteFields": "ALWAYS",
        "RecordDelimiter": RECORD_DELIM,
        "FieldDelimiter": FIELD_DELIM,
        "QuoteCharacter": QUOTE_CHAR,
        "QuoteEscapeCharacter": QUOTE_ESCAPE_CHAR
    }
}
JSONL_OUTPUT_SERIALIZATION = {"JSON": {"RecordDelimiter": RECORD_DELIM, }}


def register_csv_dialect():
    csv.register_dialect(
        INTERNAL_CSV_DIALECT_NAME,
        delimiter=FIELD_DELIM,
        doublequote=False,    # Escape quote instead of doubling
    # https://docs.python.org/3/library/csv.html#csv-fmt-params
    # The reader ignores this field and is hardcode for '\n' (2020-06-26)
        lineterminator=RECORD_DELIM,
        quotechar=QUOTE_CHAR,
        escapechar=QUOTE_ESCAPE_CHAR)
    return INTERNAL_CSV_DIALECT_NAME


RESTKEY = '_X_EXTRA_FIELDS'    # This shouldn't happen


def add_compression_type(input_serial, key):
    compression_map = {".gz": "GZIP", ".bz2": "BZIP2"}
    name, ext = os.path.splitext(key.lower())
    ctype = compression_map.get(ext, None)
    if ctype is not None:
        input_serial['CompressionType'] = ctype


#
# TODO: This code needs a thorough reading and rewriting


def s3_select_parser(full_path, in_stream, input_serial_json, cols, query,
                     file_type):

    try:
        input_serial = json.loads(input_serial_json)
        add_compression_type(input_serial, full_path)
        file_record_num = 0
        num_cols = len(cols.names)

        # Switch used for perf testing
        use_csv = False

        if use_csv or file_type == "CSV":
            row_gen = in_stream.rows(input_serial, query,
                                     CSV_OUTPUT_SERIALIZATION)
            register_csv_dialect()

            def row_gen_empty_csv_record_padding():
                for row in row_gen:
                    if row == RECORD_DELIM:
                        # Make all FNFs
                        yield FIELD_DELIM.join([
                            f'{QUOTE_CHAR}{QUOTE_CHAR}'
                            for _ in range(num_cols)
                        ]) + RECORD_DELIM
                    else:
                        yield row

            # Python ignores blank rows...
            row_reader = csv.DictReader(
                row_gen_empty_csv_record_padding(),
                fieldnames=cols.names,
                restkey=RESTKEY,    # Shouldn't happen
                restval="",    # Yield Empty string for missing fields.
                dialect=INTERNAL_CSV_DIALECT_NAME)
        elif file_type in ["JSONL", "PARQUET"]:
            row_gen = in_stream.rows(input_serial, query,
                                     JSONL_OUTPUT_SERIALIZATION)

            def jsonl_dict_gen():
                for row in row_gen:
                    yield json.loads(row)

            row_reader = jsonl_dict_gen()
        else:
            raise ValueError(f"File type '{file_type}' is not supported")

        for row in row_reader:
            icv = ""
            record = {}
            row_casted = {}
            file_record_num += 1
            try:
                row_casted = xpu_host.cast_columns(row, cols.names, cols.types)
                # TODO: Could add field size limit testing in cast_columns
            except ValueError as e:
                # ValueErrors are raised if we get an ICV while casting.
                icv = f"ERROR: {e}"
            except Exception as e:
                # These are things that shouldn't happen.
                eid = str(uuid.uuid4())
                logger.error(f"eid={eid}: S3 Parser Row Error: {e}")
                logger.error(f"eid={eid}: DATA: {row}")
                logger.error(f"eid={eid}: NAMES: {cols.names}")
                logger.error(f"eid={eid}: TYPES: {cols.types}")
                logger.error(f"eid={eid}: Trackback: {traceback.format_exc()}")
                raise RuntimeError("An internal error has occured")
            finally:
                path_val = ""
                # Drop data that will not be needed for the destination table
                if icv != "":
                    row = f'[TRUNCATED] {str(row)[0:1000]}'
                    row_casted = {name: None for name in cols.names}
                    path_val = full_path
                else:
                    row = ""

                # XXX The last ugly part is this record stuff here...
                record.update(row_casted)
                record[FRN_COLUMN_NAME] = file_record_num
                record[ICV_COLUMN_NAME] = icv
                record[SRC_COLUMN_NAME] = str(row)
                record[PATH_COLUMN_NAME] = path_val

                # TODO: We need to impose limits on the fields and the total
                # record size. Ideally this parser should be done in C++ where
                # we would have access to the limits of XCE and could enforce
                # things there. I am not sure what to do here. :facepalm:
                yield record

    except Exception as file_exception:
        # XXX We might be able to reach this mid file if the S3 Select reader
        # raises an exception in the middle. Hrmm...
        record = {}
        record[FRN_COLUMN_NAME] = None
        record[ICV_COLUMN_NAME] = f"Unable to process file: {file_exception}"
        record[SRC_COLUMN_NAME] = "UNKNOWN"
        record[PATH_COLUMN_NAME] = full_path
        logger.error(f'S3 Parser Error: msg="failed to parse file", '
                     f'path="{full_path}", exception="{file_exception}"')
        logger.error(f'S3 Parser Error: path="{full_path}", '
                     f'tracback: {traceback.format_exc()}')
        yield record
