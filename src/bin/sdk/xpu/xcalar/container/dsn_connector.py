import json
import traceback


def connect(configStream):
    conn = None
    try:
        cfg = json.loads(configStream.read())
        dbtype = cfg["dbtype"]
        host = cfg["host"]
        port = int(cfg["port"])
        dbname = cfg["dbname"]
        user = cfg["uid"]
        psw_provider = cfg["psw_provider"]
        psw = cfg["psw_arguments"]
        auth_mode = cfg["auth_mode"]

        # if password is a dict rather than strting -
        # invoke module.method encoded in the dict (e.g. hashivault, psw_provider) to retrieve the password
        if not psw_provider:
            psw = None
        elif psw_provider != "plaintext":
            module = psw_provider.split(".")
            if (len(module) != 2):
                raise Exception(
                    "Module should be of form <modulename.function>")
            exec("import {}".format(module[0]))
            provider = eval(psw_provider)
            psw = provider(psw)

        # dynamicaly load required py library and establish connection
        if dbtype == "Oracle":
            import cx_Oracle
            connString = "{}:{}/{}".format(host, port, dbname)
            conn = cx_Oracle.connect("{}/{}@{}".format(user, psw, connString))
        elif dbtype == "MySQL":
            import pymysql
            conn = pymysql.connect(
                host=host, user=user, port=port, passwd=psw, db=dbname)
        elif dbtype == "PG":
            import psycopg2
            conn = psycopg2.connect(
                dbname=dbname, user=user, host=host, password=psw, port=port)
        elif dbtype == "Hive":
            from pyhive import hive
            if not auth_mode:
                conn = hive.Connection(
                    database=dbname,
                    host=host,
                    port=port,
                    username=user,
                    password=psw)
            else:
                if auth_mode.startswith("KERBEROS:"):
                    auth, kerberos_service_name = auth_mode.split(":")
                    conn = hive.Connection(
                        database=dbname,
                        host=host,
                        port=port,
                        username=user,
                        password=psw,
                        auth=auth,
                        kerberos_service_name=kerberos_service_name)
                else:
                    conn = hive.Connection(
                        database=dbname,
                        host=host,
                        port=port,
                        username=user,
                        password=psw,
                        auth=auth_mode)
        elif dbtype == "MSSQL":
            import pyodbc
            driver = 'ODBC Driver 17 for SQL Server'
            connString = 'DRIVER={0};SERVER={1},{2};DATABASE={3};UID={4};PWD={5}'.format(
                driver, host, port, dbname, user, psw)
            conn = pyodbc.connect(connString)
        elif dbtype == "TD":
            import pyodbc
            driver = 'Teradata ODBC DSN'
            connString = 'DSN={0};UID={1};PWD={2};DBCName={3};PORT={4};'.format(
                driver, user, psw, host, port)
            conn = pyodbc.connect(connString)
        else:
            raise Exception("Unknown dbtype")
    except Exception as e:
        raise Exception("Could not connect to {}; Error:{}".format(dbtype, e))
    return conn


# this function takes a query and a DSN name as parameters, configStream currently not used, reserved for parameterization
# all dsn configurations are stored in one definiton file - and can be dynamically switched based on dsn parameter
def ingestFromDB(query, configStream):
    cursor = None
    connection = None
    try:
        connection = connect(configStream)
        cursor = connection.cursor()
        cursor.execute(query)
        columnNames = [x[0] for x in cursor.description]
        while True:
            rows = cursor.fetchmany()
            if not rows:
                break
            records = [dict(zip(columnNames, r)) for r in rows]
            yield from records
    except Exception as e:
        yield {"error!": str(e), "traceback": traceback.format_exc()}
    finally:
        if connection is not None:
            cursor.close()
            connection.close()
