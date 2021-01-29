import math
import logging
import os
import xcalar.container.driver.base as driver
import xcalar.container.context as ctx
import snappy
import json
import struct
import zlib

logger = logging.getLogger("xcalar")

# XXX TODO This can be parameterized.
MAX_WRITE_BUF_LEN = 64 * 1024 * 1024

SER_VERSION = 1
SER_HEADER_MAGIC = 0xc001cafe
SER_FOOTER_MAGIC = 0xfffffffe


# Return checksum
# block: Block on which to compute checksum
# file_check_sum: File checksum so far
def ret_checksum(block, file_check_sum):
    check_sum = zlib.adler32(block, file_check_sum)
    return check_sum & 0xffffffff


# Write the file header. Header includes version, magic.
# f: file stream
def write_header(f):
    packed_header = struct.pack("II", SER_VERSION, SER_HEADER_MAGIC)
    f.write(packed_header)


# Write the file footer. Footer includes magic, checksum (excludes header),
# and version.
# f: file stream
# file_check_sum: File checksum so far
def write_footer(f, file_check_sum):
    packed_footer = struct.pack("III", SER_FOOTER_MAGIC, file_check_sum,
                                SER_VERSION)
    f.write(packed_footer)


# Helper method to write the table contents.
# columnar_dict: Records organized in a columnar fashion
# num_records: Number of records to write
# f: file stream
# file_check_sum: File checksum so far
# Returns,
# file_check_sum: File checksum including the write
def write_helper(columnar_dict, num_records, f, file_check_sum):
    for k, v in columnar_dict.items():
        ser = json.dumps(columnar_dict[k])
        ser_comp = snappy.compress(ser)
        buf_len = len(ser_comp)
        ser_comp_packed = struct.pack("II%ds" % (buf_len), num_records,
                                      len(ser_comp), ser_comp)
        f.write(ser_comp_packed)
        file_check_sum = ret_checksum(ser_comp_packed, file_check_sum)

    return file_check_sum


@driver.register_export_driver(
    name="snapshot_export_driver", is_builtin=True, is_hidden=True)
@driver.param(
    name="directory_path",
    type=driver.STRING,
    desc="directory to write all exported files to.")
@driver.param(
    name="file_base",
    type=driver.STRING,
    desc="file name to use for the exported files inside the "
    "directory. For example, for 'myfile', it will produce "
    "files named like 'myfile000' and 'myfile001'. "
    "This defaults to the table name.",
    optional=True)
def driver(table, directory_path, file_base=None):
    """Internal only. Exports data for the purposes of snapshotting published
    tables"""
    columns = [c["headerAlias"] for c in table.columns()]
    xpu_id = ctx.get_xpu_id()

    # Format the number in the file so that it has leading zeros
    xpu_cluster_size = ctx.get_xpu_cluster_size()
    num_digits = int(math.log10(xpu_cluster_size))
    part_string = str(xpu_id).zfill(num_digits)
    file_name = "{}{}".format(file_base, part_string)
    file_path = os.path.join(directory_path, file_name)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_check_sum = 0

    with open(file_path, "wb") as f:
        write_header(f)

        # Write out the table in a columnar fashion to get optimal compression
        write_buffer_length = 0
        num_records = 0
        columnar_dict = {}
        for col in columns:
            columnar_dict[col] = []
        needs_init = False

        # Get table partitioned rows iterator for this XPU
        part_rows = table.partitioned_rows()
        for row in part_rows:
            if needs_init:
                write_buffer_length = 0
                num_records = 0
                for k in columnar_dict:
                    columnar_dict[k] = []
                needs_init = False

            row_list = list(row.values())
            col_idx = 0
            for v in columnar_dict.values():
                field = row_list[col_idx]
                write_buffer_length += len(bytes(str(field), 'utf8'))
                v.append(field)
                col_idx += 1
            num_records += 1

            # Write out the buffers in a columnar fashion
            if write_buffer_length >= MAX_WRITE_BUF_LEN:
                file_check_sum = write_helper(columnar_dict, num_records, f,
                                              file_check_sum)
                needs_init = True

        # Empty the left over buffers
        if write_buffer_length != 0:
            file_check_sum = write_helper(columnar_dict, num_records, f,
                                          file_check_sum)

        write_footer(f, file_check_sum)

    logger.info(
        "Snapshot driver export, xpu_cluster_size: {}, xpu_id: {}, file: {}, checksum: {}"
        .format(xpu_cluster_size, xpu_id, file_path, file_check_sum))
