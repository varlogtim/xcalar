import psycopg2
import xcalar.container.driver.base as driver
import xcalar.compute.util.imd.imd_constants as ImdConstant
import xcalar.container.cluster

UPSERT_SQL = """
INSERT INTO {table_name} ({col_names})
VALUES ({col_values})
ON CONFLICT ({pk}) DO UPDATE
SET {update_cols}
"""
DELETE_SQL = """
DELETE FROM {table_name}
WHERE {pk} = {pk_value};
"""


@driver.register_export_driver(name="pg_imd")
@driver.param(
    name="db_user", type=driver.STRING, desc="user to the postgres db")
@driver.param(
    name="db_pass", type=driver.STRING, desc="passwd to the postgres db")
@driver.param(
    name="db_host",
    type=driver.STRING,
    desc="host address for the postgres db")
@driver.param(
    name="db_port",
    type=driver.STRING,
    desc="port address for the postgres db")
@driver.param(
    name="db", type=driver.STRING, desc="database name to connect to")
@driver.param(
    name="db_table", type=driver.STRING, desc="table name in the postgres db")
@driver.param(
    name="db_pk",
    type=driver.STRING,
    desc="primary key for the tablein the postgres db")
def driver(table, db_user, db_pass, db_host, db_port, db, db_table, db_pk):
    # This code runs on all XPUs on all nodes, but for only creating a single
    # db connection per node, we are going to do that from just a local master XPU.
    # Thus, all other XPUs can now exit
    cluster = xcalar.container.cluster.get_running_cluster()
    if not cluster.is_local_master():
        return

    try:
        db_conn = psycopg2.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port,
            database=db)
    except psycopg2.Error as e:
        raise RuntimeError("Error while connection to PostgreSQL", e)
    cur = db_conn.cursor()
    try:
        schema = {s['name']: s['type'] for s in table.schema}
        for row in table.local_node_rows():
            if row[ImdConstant.Opcode] == 1:
                kvs = {}
                for key in row:
                    if key == ImdConstant.Opcode or key == ImdConstant.Rankover or row[
                            key] is None:
                        continue
                    kvs[key] = row[key]
                    if schema[key] == 'DfString':
                        kvs[key] = "'{}'".format(kvs[key])
                statement = UPSERT_SQL.format(
                    table_name=db_table,
                    col_names=', '.join(kvs.keys()),
                    col_values=', '.join(map(str, kvs.values())),
                    update_cols=', '.join([
                        "{}={}".format(e[0], e[1])
                        for e in zip(kvs.keys(), kvs.values())
                    ]),
                    pk=db_pk)
            elif row[ImdConstant.Opcode] == 0:
                pk_value = row[db_pk]
                if schema[db_pk] == 'DfString':
                    pk_value = "'{}'".format(pk_value)
                statement = DELETE_SQL.format(
                    table_name=db_table, pk=db_pk, pk_value=pk_value)
            else:
                raise RuntimeError("Invalid record: ", row)
            cur.execute(statement)
    finally:
        db_conn.commit()
        cur.close()
        db_conn.close()


def placeholder():
    return 1    # this is needed because otherwise this UDF module just gets swallowed
