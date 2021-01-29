import json
import traceback
import logging
import math

from xcalar.external.client import Client
from xcalar.compute.util.utils import connect_snowflake
from xcalar.container.cluster import get_running_cluster

try:
    from snowflake.connector import DictCursor
except:
    pass

logger = logging.getLogger("xcalar")
LIMIT_ROW = 10000

def fetchRow(cursor):
    while True:
        rows = cursor.fetchmany()
        if not rows:
            break
        yield from rows
    cursor.close()

def ingestFromSnowflake(query, kvstoreKey, configStream):
    cluster = get_running_cluster()
    my_node_id = cluster.my_node_id
    if cluster.is_local_master() and my_node_id == 0:
        connection = connect_snowflake(json.loads(configStream.read()))
        if (kvstoreKey != ""):
            client = Client(bypass_proxy=True)
            kvstore = client.global_kvstore()
            query = kvstore.lookup(kvstoreKey)
            logger.info("Predicate pushdown query: {}".format(query))
            kvstore.delete(kvstoreKey)
        cursor = connection.cursor(DictCursor)
        cursor.execute(query)
        yield from fetchRow(cursor)
        connection.close()

def partitionMap(num_rows, num_xpu):
    step_size = max(LIMIT_ROW, math.ceil(num_rows/num_xpu))
    partition_map = [i for i in range(0, num_rows, step_size)]
    return partition_map, step_size

def ingestFromSnowflakeTable(table_name, configStream):
    cluster = get_running_cluster()
    my_node_id = cluster.my_node_id
    query = f"select * from {table_name}"
    if cluster.is_local_master() and my_node_id == 0:
        connection = connect_snowflake(json.loads(configStream.read()))
        cursor = connection.cursor(DictCursor)
        cursor.execute(query)
        yield from fetchRow(cursor)
        connection.close()

'''
def ingestFromSnowflakeTableParallel(table_name, configStream):
    cluster = get_running_cluster()
    my_node_id = cluster.my_node_id
    my_xpu_id = cluster.my_xpu_id
    query = f"select * from {table_name}"
    if cluster.is_local_master() and my_node_id == 0:
        connection = connect_snowflake(json.loads(configStream.read()))
        cursor = connection.cursor(DictCursor)
        cursor.execute(query)
        # calculate the partition_map based on num of row and num of xpu
        num_rows = cursor.rowcount
        xpus_per_node = cluster.xpus_per_node
        total_xpus = sum(xpus_per_node)
        partition_map, step_size = partitionMap(num_rows, total_xpus)
        cluster.broadcast_msg(json.dumps({"partition_map":partition_map,
                                "step_size" : step_size}))
        logger.info("Successfully broadcasted!")
        new_query = query + " " + "LIMIT {} OFFSET {}".format(step_size, partition_map[my_xpu_id])
        cursor.execute(new_query)
        yield from fetchRow(cursor)
        connection.close()
    else:
        partition_info = json.loads(cluster.recv_msg())
        partition_map = partition_info["partition_map"]
        if my_xpu_id > len(partition_map) - 1:
            return
        else:
            new_query = query + " " + "LIMIT {} OFFSET {}".format(partition_info["step_size"], partition_map[my_xpu_id])
            connection = connect_snowflake(json.loads(configStream.read()))
            cursor = connection.cursor(DictCursor)
            cursor.execute(new_query)
            yield from fetchRow(cursor)
            connection.close()
'''