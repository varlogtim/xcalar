import csv
import math
import os
import codecs

import xcalar.container.context as ctx
import xcalar.container.driver.base as driver
import xcalar.container.cluster
from xcalar.container.utils import parse_utf8_delimiter

# Set up logging
logger = ctx.get_logger()

Utf8Encoder = codecs.getwriter("utf-8")


@driver.register_export_driver(name="multiple_csv", is_builtin=True)
@driver.param(
    name="target", type=driver.TARGET, desc="Destination target for the file")
@driver.param(
    name="directory_path",
    type=driver.STRING,
    desc="Directory to write all exported CSV files to; "
    "example '/datasets/mydataset'")
@driver.param(
    name="file_base",
    type=driver.STRING,
    desc="File name to use for the exported files inside the "
    "directory. For example, for 'myfile', it will produce "
    "files named like 'myfile000.csv' and 'myfile001.csv'.")
@driver.param(
    name="header",
    type=driver.BOOLEAN,
    optional=True,
    desc="Whether to include a header line as the first line of the "
    "file")
@driver.param(
    name="field_delim",
    type=driver.STRING,
    desc="What delimiter should separate the fields of the file. "
    "Defaults to the tab character. E.g., '\\t', '\\u2566', etc..",
    optional=True)
@driver.param(
    name="record_delim",
    type=driver.STRING,
    desc="What delimiter should separate the records of the file. "
    "Defaults to a newline character. E.g., '\\n', '\\u02A0', etc..",
    optional=True)
@driver.param(
    name="quote_delim",
    type=driver.STRING,
    desc="What delimiter should be used as the quoting character "
    "for the file. Defaults to '\"'. E.g., '\"', \"'\", '\\u05D9', etc..",
    optional=True)
def driver(table,
           target,
           directory_path,
           file_base=None,
           header=True,
           field_delim='\t',
           record_delim='\n',
           quote_delim='"'):
    """Export data to many CSV files in the specified target. The number of files
    is determined at runtime, but is correlated with the number of nodes in the
    Xcalar cluster and the number of cores on those nodes. Supports use of single
    unicode characters as field, record and quote characters.
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

    # Parse escaped delimiter strings, e.g., '\\n', '\\u2566'
    field_delim = parse_utf8_delimiter(field_delim)
    record_delim = parse_utf8_delimiter(record_delim)
    quote_delim = parse_utf8_delimiter(quote_delim)

    # check if this partition has any records
    if table.partitioned_row_count() == 0:
        return

    # Now we create the file itself; this should create any directories needed
    with target.open(file_path, "wb") as f:
        utf8_file = Utf8Encoder(f)
        writer = csv.DictWriter(
            utf8_file,
            columns,
            delimiter=field_delim,
            quotechar=quote_delim,
            lineterminator=record_delim,
            extrasaction="ignore")

        if header:
            writer.writeheader()

        writer.writerows(table.partitioned_rows())
