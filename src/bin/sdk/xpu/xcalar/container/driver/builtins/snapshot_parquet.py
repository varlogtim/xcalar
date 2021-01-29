import math
import os
import io
import re
import hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

import xcalar.container.context as ctx
import xcalar.container.driver.base as driver
import xcalar.container.cluster
from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT

# Set up logging
logger = ctx.get_logger()


@driver.register_export_driver(
    name="snapshot_parquet", is_builtin=True, is_hidden=True)
@driver.param(
    name="target",
    type=driver.TARGET,
    desc="Select a destination connector for the exported files.")
@driver.param(
    name="directory_path",
    type=driver.STRING,
    desc="Enter a destination directory path for the exported files. "
    "For example, '/datasets/export'")
@driver.param(
    name="file_base",
    type=driver.STRING,
    desc="Enter a file name without the extension for the exported files. "
    "For example, entering 'myfile' will produce files 'myfile000.parquet', 'myfile001.parquet' and so on.",
    optional=True)
@driver.param(
    name="compression",
    type=driver.STRING,
    desc="Specify the compression codec, either on a general basis "
    "or per-column. Valid values: {‘NONE’, ‘SNAPPY’, ‘GZIP’, ‘LZO’, "
    "‘BROTLI’, ‘LZ4’, ‘ZSTD’}. Defaults to SNAPPY",
    optional=True)
def driver(table, target, directory_path, file_base, compression='SNAPPY'):
    """
    Export data in a parquet format.
    XXX Have issues with type inference of prefixed fields, is good to only
    to use by imd sdk apis.
    """
    # check if this partition has any records, allow only one to write a empty file
    cluster = xcalar.container.cluster.get_running_cluster()
    if table.partitioned_row_count() == 0 and not cluster.is_local_master():
        return
    # check valid compression
    valid_compressions = {
        'NONE', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4', 'ZSTD'
    }
    if compression not in valid_compressions:
        raise ValueError(
            "Invalid compression {} provided, should be any one of '{}'".
            format(compression, ", ".join(valid_compressions)))

    columns = [c["headerAlias"] for c in table.columns()]
    cluster = xcalar.container.cluster.get_running_cluster()
    xpu_id = cluster.my_xpu_id

    # Format the number in the file so that it has leading zeros
    xpu_cluster_size = cluster.total_size
    num_digits = int(math.log10(xpu_cluster_size))
    part_string = str(xpu_id).zfill(num_digits)

    file_name = "{}{}".format(file_base, part_string)
    file_path = os.path.join(directory_path, file_name + '.parquet')
    checksum_path = os.path.join(directory_path, file_name + '.checksum')

    # Parse escaped delimiter strings, e.g., '\\n'
    field_delim = '\t'
    record_delim = '\n'
    quote_delim = '"'
    pattern = r'(\t|\n|^)(\"\")(\t|\n|$)'

    # Now we create the file itself; this should create any directories needed
    records = "".join(
        table.partitioned_rows(
            format=DfFormatTypeT.DfFormatInternal,
            field_delim=field_delim,
            record_delim=record_delim,
            quote_delim=quote_delim))
    # Properly handle None and empty string
    while re.search(pattern, records):
        records = re.sub(pattern, r'\1\\\\"XcEmpty\\\\"\3', records)
    df = pd.read_csv(
        io.StringIO(records),
        sep=field_delim,
        encoding='utf-8',
        doublequote=False,
        escapechar='\\',
        quotechar=quote_delim,
        names=columns,
        header=None,
        keep_default_na=False,
        dtype=str)
    df = df.replace(r'^$', np.nan, regex=True).replace('\\"XcEmpty\\"', "")
    arrow_table = pa.Table.from_pandas(df=df, preserve_index=False)
    out_buf = io.BytesIO()
    pq.write_table(arrow_table, out_buf, compression=compression)
    out_buf.seek(0)

    # checksum
    checksum = hashlib.md5(out_buf.read()).hexdigest()
    out_buf.seek(0)

    with target.open(file_path, 'wb') as fp1, target.open(checksum_path,
                                                          'w') as fp2:
        fp1.write(out_buf.read())
        fp2.write(checksum)
