import math
import logging
import os
import json
from json import dumps
import io
import time

import xcalar.container.driver.base as driver
from xcalar.container.cluster import get_running_cluster
import xcalar.container.context as ctx

from xcalar.compute.util.utils import connect_snowflake
from xcalar.compute.coretypes.DataFormatEnums.ttypes import DfFormatTypeT

MAX_BATCH_SIZE = 16000

# XXX Snowflake only allows up to 20 parallel locks on resources such as a table
MAX_PARALLEL_CONNECTIONS = 20

logger = logging.getLogger("xcalar")

typeMapping = {
    "DfInt32": "int",
    "DfInt64": "int",
    "DfUInt32": "int",
    "DfUInt64": "int",
    "DfFloat32": "float",
    "DfFloat64": "float",
    "DfString": "string",
    "DfBoolean": "boolean",
    "DfTimespec": "timestamp",
    "DfMoney": "number(12,2)"
}

def _partition_map(num_rows, num_xpu):
    step_size = max(MAX_BATCH_SIZE, math.ceil(num_rows/num_xpu))
    partition_map = [(i, min(step_size, num_rows-i)) for i in range(0, num_rows, step_size)]
    return partition_map


def export_table(cursor, table, target, new_table_name, partition_map, my_xpu_id):
    start_row, partitioned_row_count = partition_map[my_xpu_id]
    field_delim = "|"
    record_delim = '\n'
    quote_delim = '"'

    row_generator = table._local_cursor(
        start_row=start_row,
        num_rows=partitioned_row_count,
        buf_size=100,
        format=DfFormatTypeT.DfFormatInternal,
        field_delim=field_delim,
        record_delim=record_delim,
        quote_delim=quote_delim)

    logger.info(f"XPU ID: {my_xpu_id} started. Start Row: {start_row}. Num Rows: {partitioned_row_count}")
    start = time.time()
    rows = [repr(tuple(row.split(field_delim))).replace('None', 'NULL') for row in row_generator]
    pyWorkTime = time.time() - start
    sfExportTime = 0
    for start_idx in range(0, partitioned_row_count, MAX_BATCH_SIZE):
        end_idx = min(start_idx + MAX_BATCH_SIZE, partitioned_row_count)
        row_str = ", ".join(rows[start_idx: end_idx])
        insert_command = f"insert into {new_table_name} values {row_str}"
        start = time.time()
        cursor.execute(insert_command)
        sfExportTime += (time.time() - start)

    logger.info(f"XPU ID: {my_xpu_id} done exporting to snowflake! Python processing time: {pyWorkTime}, SF execute time: {sfExportTime}")

    cursor.close()

@driver.register_export_driver(name="snowflake_export", desc="Snowflake Export Driver", is_builtin=True)
@driver.param(
    name="target",
    type=driver.TARGET,
    desc="Select Snowflake target for export")
@driver.param(
    name="new_table_name",
    type=driver.STRING,
    desc="Name of the table to be created in Snowflake ")
def driver(table,
           target,
           new_table_name):
    cluster = get_running_cluster()
    my_node_id = cluster.my_node_id
    my_xpu_id = cluster.my_xpu_id

    if cluster.is_local_master() and my_node_id == 0:
        conn = connect_snowflake(target._config)
        cur = conn.cursor()
        table_columns = ','.join(['{0} {1}'.format(col["name"],
                                                    typeMapping.get(col["type"], "string"))
                                  for col in table.schema])
        try:
            cur.execute("""CREATE TABLE {}
                            ({})""".format(new_table_name, table_columns))
        except:
            raise
        xpus_per_node = cluster.xpus_per_node
        num_xpus = sum(xpus_per_node)
        total_rows = 0
        for _, num_rows in enumerate(table._meta.numRowsPerNode):
            total_rows += num_rows

        partition_map = _partition_map(total_rows,  min(MAX_PARALLEL_CONNECTIONS, num_xpus))

        cluster.broadcast_msg(json.dumps({"partition_map":partition_map}))
        export_table(cur, table, target, new_table_name, partition_map, my_xpu_id)
        conn.close()
    else:
        partition_info = json.loads(cluster.recv_msg())
        partition_map = partition_info["partition_map"]
        if my_xpu_id > len(partition_map) - 1:
            return
        conn = connect_snowflake(target._config)
        cur = conn.cursor()
        export_table(cur, table, target, new_table_name, partition_map, my_xpu_id)
        conn.close()



   