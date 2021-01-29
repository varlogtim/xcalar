import math
import os
import codecs

import xcalar.container.context as ctx
import xcalar.container.driver.base as driver
import xcalar.container.cluster
from xcalar.container.utils import parse_ascii_delimiter

from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT

# Set up logging
logger = ctx.get_logger()

Utf8Encoder = codecs.getwriter("utf-8")


@driver.register_export_driver(name="fast_csv", is_builtin=True)
@driver.param(
    name="target", type=driver.TARGET, desc="destination target for the file")
@driver.param(
    name="directory_path",
    type=driver.STRING,
    desc="directory to write all exported CSV files to.")
@driver.param(
    name="file_base",
    type=driver.STRING,
    desc="file name to use for the exported files inside the "
    "directory. For example, for 'myfile', it will produce "
    "files named like 'myfile000.csv' and 'myfile001.csv'. "
    "This defaults to the table name.",
    optional=True)
@driver.param(
    name="field_delim",
    type=driver.STRING,
    desc="What single ASCII character should separate the fields of the file. "
    "Defaults to the tab character. E.g., '\\t', '\\x09', etc..",
    optional=True)
@driver.param(
    name="record_delim",
    type=driver.STRING,
    desc="What single ASCII character should separate the records of "
    "the file. Defaults to a newline. E.g., '\\n', '\\x0A', etc..",
    optional=True)
@driver.param(
    name="quote_delim",
    type=driver.STRING,
    desc="What single ASCII character should be used as the quoting character "
    "for the file. Defaults to '\"'. E.g., '\"', \"'\", '\\x5E', etc..",
    optional=True)
def driver(table,
           target,
           directory_path,
           file_base=None,
           field_delim='\t',
           record_delim='\n',
           quote_delim='"'):
    """Export data in a Xcalar proprietary CSV format. Supports single ASCII
    characters as field, record and quote delimiters.
    """
    columns = [c["headerAlias"] for c in table.columns()]
    cluster = xcalar.container.cluster.get_running_cluster()
    xpu_id = cluster.my_xpu_id

    # Format the number in the file so that it has leading zeros
    xpu_cluster_size = cluster.total_size
    num_digits = int(math.log10(xpu_cluster_size))
    part_string = str(xpu_id).zfill(num_digits)

    file_name = "{}{}.csv".format(file_base, part_string)
    file_path = os.path.join(directory_path, file_name)

    # Parse escaped delimiter strings, e.g., '\\n'
    field_delim = parse_ascii_delimiter(field_delim)
    record_delim = parse_ascii_delimiter(record_delim)
    quote_delim = parse_ascii_delimiter(quote_delim)

    # check if this partition has any records
    if table.partitioned_row_count() == 0:
        return

    # Now we create the file itself; this should create any directories needed
    with target.open(file_path, "wb") as f:
        utf8_file = Utf8Encoder(f)
        for row_csv in table.partitioned_rows(
                format=DfFormatTypeT.DfFormatCsv,
                field_delim=field_delim,
                record_delim=record_delim,
                quote_delim=quote_delim):
            utf8_file.write(row_csv)
