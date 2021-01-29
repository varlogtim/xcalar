import csv
import json
import os
import io
import imp

from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Udf import Udf

import xcalar.container.driver.base as driver
import xcalar.container.context as ctx

# Set up logging
logger = ctx.get_logger()

BUF_SIZE = 16777216


def _get_udf_func(export_udf):
    full_udf_path = "/sharedUDFs/{}".format(export_udf)
    xcalarApi = XcalarApi(bypass_proxy=True)

    udf = Udf(xcalarApi)
    module_source = udf.get(full_udf_path).source

    user_module = imp.new_module("userModule")
    exec(module_source, user_module.__dict__)
    # For 'apps', the function is always "main"
    user_function = getattr(user_module, "main", None)
    if not user_function:
        raise ValueError("Export App '{}' does not have a main function".
                         format(user_module))
    return user_function


def _render_udf_input(file_path, file_contents):
    return json.dumps({"filePath": file_path, "fileContents": file_contents})


def _render_file_path(directory, file_name, node, part, chunk):
    file_base, ext = os.path.splitext(file_name)
    subdir = file_base
    file_name = "{file_base}-n{node}-c{chunk}-p{part}{ext}".format(
        file_base=file_base, node=node, chunk=chunk, part=part, ext=ext)
    return os.path.join(directory, subdir, file_name)


def _render_header_path(directory, file_name):
    file_base, ext = os.path.splitext(file_name)
    subdir = file_base
    file_name = "{file_base}-header{ext}".format(file_base=file_base, ext=ext)
    return os.path.join(directory, subdir, file_name)


@driver.register_export_driver(name="legacy_udf", is_builtin=True)
@driver.param(
    name="directory_path",
    type=driver.STRING,
    desc="directory to write all exported CSV files to.")
@driver.param(
    name="export_udf",
    type=driver.STRING,
    desc="Name of the Shared Xcalar UDF module to use when exporting.")
@driver.param(
    name="file_name",
    type=driver.STRING,
    desc="file name to use for the exported files inside the "
    "directory. For example, for 'myfile', it will produce "
    "files named like 'myfile000.csv' and 'myfile001.csv'. "
    "This defaults to the table name.")
@driver.param(
    name="header",
    type=driver.STRING,
    desc="What should be done with the header of the CSV. "
    "Valid options are 'none', 'every' and 'separate'.")
@driver.param(
    name="fieldDelim",
    type=driver.STRING,
    desc="What delimiter should separate the fields of the file. "
    "Defaults to ','.",
    optional=True)
@driver.param(
    name="recordDelim",
    type=driver.STRING,
    desc="What delimiter should separate the records of the file. "
    "Defaults to a newline.",
    optional=True)
@driver.param(
    name="quoteDelim",
    type=driver.STRING,
    desc="What delimiter should be used as the quoting character "
    "for the file. Defaults to '\"'",
    optional=True)
def driver(table,
           directory_path,
           export_udf,
           file_name,
           header,
           fieldDelim=',',
           recordDelim='\n',
           quoteDelim='"'):
    """Export data in the legacy style. This will render a size-limited CSV
    file before passing it off to the provided UDF. The provided UDF is
    responsible for actually exporting the data to some sort of persistent
    store. Advanced users should implement custom drivers to achieve similar
    functionality without the limitations of this driver."""
    # validate parameters
    if header not in ['none', 'every', 'separate']:
        raise ValueError("header type '{}' is not valid".format(header))

    # gather dynamic constants
    columns = [c["headerAlias"] for c in table.columns()]
    xpu_id = ctx.get_xpu_id()
    node_id = ctx.get_node_id(xpu_id)

    file_path = os.path.join(directory_path, file_name)

    buf = io.StringIO()

    writer = csv.DictWriter(
        buf,
        columns,
        delimiter=fieldDelim,
        quotechar=quoteDelim,
        lineterminator=recordDelim,
        extrasaction="ignore")

    udf_func = _get_udf_func(export_udf)

    if header == "separate" and xpu_id == 0:
        # Make the dedicated header file
        writer.writeheader()

        # Write out the file to our UDF
        buf.seek(0)
        buf_str = buf.read()
        file_path = _render_header_path(directory_path, file_name)
        udf_in = _render_udf_input(file_path, buf_str)
        udf_func(udf_in)

    # check if table partition has any records
    if table.partitioned_row_count() == 0:
        return

    saved_row = None
    last_pos = None
    row_iter = iter(table.partitioned_rows())
    file_num = 0
    finished = False
    while not finished:
        # Set up new file
        buf.seek(0)
        buf.truncate()

        if header == "every":
            writer.writeheader()

        if saved_row:
            # Check if we have a spillover row from the last buf
            writer.writerow(saved_row)
            saved_row = None

        # Dump records into this file until it's full
        try:
            while True:
                row = next(row_iter)
                last_pos = buf.tell()
                writer.writerow(row)
                new_pos = buf.tell()
                if last_pos > 0 and new_pos > BUF_SIZE:
                    saved_row = row
                    break
            # Cut off that last row that caused the spillover
            if saved_row is not None:
                buf.seek(last_pos)
                buf.truncate()
        except StopIteration:
            # we finished the stream of data
            finished = True

        # Write out the file we just filled
        buf.seek(0)
        buf_str = buf.read()
        file_path = _render_file_path(directory_path, file_name, node_id,
                                      xpu_id, file_num)
        udf_in = _render_udf_input(file_path, buf_str)

        udf_func(udf_in)
        file_num += 1
