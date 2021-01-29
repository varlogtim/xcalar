import csv
import codecs

import xcalar.container.context as ctx
import xcalar.container.driver.base as driver
import xcalar.container.cluster
from xcalar.container.utils import parse_utf8_delimiter

# Set up logging
logger = ctx.get_logger()

Utf8Encoder = codecs.getwriter("utf-8")


@driver.register_export_driver(name="single_csv", is_builtin=True)
@driver.param(
    name="target", type=driver.TARGET, desc="Destination target for the file")
@driver.param(
    name="file_path",
    type=driver.STRING,
    desc="Exported file path; example '/datasets/mydataset.csv'")
@driver.param(
    name="header",
    type=driver.BOOLEAN,
    optional=True,
    desc="Whether to include a header line as the first line of the file")
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
           file_path,
           header=True,
           field_delim='\t',
           record_delim='\n',
           quote_delim='"'):
    """Export data to a single CSV file in the specified target. This is slower
    than exporting to multiple files since only some of the hardware resources
    are able to be used, thus this is best for relatively small amounts of
    data. Supports use of single unicode characters as field, record and quote
    delimiters."""
    # Get our list of columns from the table. Note that we don't care about
    # what the column names were in the original table, only what the user has
    # decided they would like to call these columns in the exported format
    columns = [c["headerAlias"] for c in table.columns()]

    # This code runs on all XPUs on all nodes, but for only creating a single
    # file, we are going to do that from just a single XPU. Thus, all other
    # XPUs can now exit
    cluster = xcalar.container.cluster.get_running_cluster()
    if not cluster.is_master():
        return

    # Parse escaped delimiter strings, e.g., '\\n', '\\u2566'
    field_delim = parse_utf8_delimiter(field_delim)
    record_delim = parse_utf8_delimiter(record_delim)
    quote_delim = parse_utf8_delimiter(quote_delim)

    # Now we create the file itself; this should create any directories needed
    with target.open(file_path, "wb") as f:
        utf8_file = Utf8Encoder(f)
        # Let's create our CSV writer object. We're using mostly default
        # arguments here. The 'extrasaction=ignore' means that any spurious
        # fields not in 'columns' will simply be ignored by the CSV writer
        writer = csv.DictWriter(
            utf8_file,
            columns,
            delimiter=field_delim,
            quotechar=quote_delim,
            lineterminator=record_delim,
            extrasaction="ignore")
        if header:
            writer.writeheader()
        # Now we create a row cursor across all the rows in the table
        all_rows_cursor = table.all_rows()
        # We could iterate over this cursor and write each row directly, but
        # the writer object has highly performant code for doing this type of
        # batch processing and it is all written in C, so we will defer to it
        writer.writerows(all_rows_cursor)
