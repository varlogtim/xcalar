import math
import os
import gzip
import time
import re
import hashlib

import xcalar.container.context as ctx
import xcalar.container.driver.base as driver
import xcalar.container.cluster

from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT

# Set up logging
logger = ctx.get_logger()

SCALE = {"K": 2**10, "M": 2**20, "G": 2**30}


@driver.register_export_driver(
    name="snapshot_csv", is_builtin=True, is_hidden=True)
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
    name="max_file_size",
    type=driver.STRING,
    desc="Limit the size (bytes) of each file on export. Must be an integer, "
    "optionally followed by scale factor; K, M, or G. e.g., 100M, 150K, etc..",
    optional=True)
def driver(table, target, directory_path, file_base=None,
           max_file_size="200M"):
    """Export data in a Xcalar proprietary CSV format. Supports single ASCII
    characters as field, record and quote delimiters.
    """

    match = re.match(r'^([0-9]+)([KMG])?', max_file_size)
    if match is None:
        raise ValueError(
            "max_file_size (bytes) must be an integer, optionally followed by scale factor: K, M, or G"
        )
    number = int(match.group(1))
    scale = match.group(2)
    max_size = number * SCALE[scale] if scale else number
    # these are values for xcalarSnapshot dialect
    header = True
    field_delim = '\t'
    record_delim = '\n'
    quote_delim = '"'

    columns = [c["headerAlias"] for c in table.columns()]
    cluster = xcalar.container.cluster.get_running_cluster()
    xpu_id = cluster.my_xpu_id

    # Format the number in the file so that it has leading zeros
    xpu_cluster_size = cluster.total_size
    num_digits = int(math.log10(xpu_cluster_size))
    part_string = str(xpu_id).zfill(num_digits)

    file_name = "{}_{}".format(file_base, part_string)
    file_path_no_ext = os.path.join(directory_path, file_name)

    # check if this partition has any records
    if table.partitioned_row_count() == 0 and not cluster.is_local_master():
        return

    header = field_delim.join(columns) + record_delim if header else ""
    header = header.encode()
    header_len = len(header)
    if max_size <= header_len:
        raise ValueError(
            f"max_file_size {max_file_size} should be greater than header length {header_len}"
        )
    rows_generator = table.partitioned_rows(
        format=DfFormatTypeT.DfFormatInternal,
        field_delim=field_delim,
        record_delim=record_delim,
        quote_delim=quote_delim)

    format_str_size = "of size {} bytes compressed, original size was {} bytes,"
    break_loop = False
    chunk_num = 1
    while not break_loop:
        # setup file path here
        file_path = "{}_{}.csv".format(file_path_no_ext, chunk_num)
        checksum_path = "{}.checksum".format(file_path)
        file_path += ".gz"
        start_time = time.monotonic()
        checksum_content = hashlib.md5()
        file_sizes_str = ""
        with gzip.GzipFile(
                fileobj=target.open(file_path, "wb"), mode="wb") as fp:
            # write header
            buf_size = header_len
            fp.write(header)
            checksum_content.update(header)
            # write data
            for rows in rows_generator:
                rows = rows.encode()
                buf_size += len(rows)
                checksum_content.update(rows)
                fp.write(rows)
                if buf_size >= max_size:
                    break
            if buf_size < max_size:
                break_loop = True
            file_sizes_str = format_str_size.format(fp.fileobj.tell(),
                                                    buf_size)
        end_time = time.monotonic()
        logger.info("Snapshot written to {} {} in {:.2f}secs".format(
            file_path, file_sizes_str, end_time - start_time))
        # write checksum
        with target.open(checksum_path, "wb") as c_f:
            c_f.write(checksum_content.hexdigest().encode())
        chunk_num += 1
