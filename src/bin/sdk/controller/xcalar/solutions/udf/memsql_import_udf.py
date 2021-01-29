import json
from xcalar.external.client import Client
import xcalar.container.context as ctx
# from xcalar.container.dsn_connector import ingestFromDB
import MySQLdb
import MySQLdb.cursors
import traceback
import logging
import time
from memsql.common import database

client = Client(bypass_proxy=True)
kvstore = client.global_kvstore()
logger = logging.getLogger('xcalar')


import sqlparse

test_sql = """select * from TaxLots_Cost t, TaxLots_Cost t1, TaxLots_Cost t2, TaxLots_Cost t3
where t.acct_no like '309%' and t.UNIT_PRICE_IN_ISSUE_CURRENCY=3
and t1.acct_no like '309%' and t1.UNIT_PRICE_IN_ISSUE_CURRENCY=4
and t2.acct_no like '309%' and t2.UNIT_PRICE_IN_ISSUE_CURRENCY=60
and t3.acct_no like '309%' and t3.UNIT_PRICE_IN_ISSUE_CURRENCY=4"""

_test_sql = "select * from TaxLots_Cost t, TaxLots_Cost t1  where t.acct_no like '309%' and cast(t.UNIT_PRICE_IN_ISSUE_CURRENCY as char) like '3%' limit 3500000"

_test_sql = "select * from TaxLots_Cost"

def add_filter(query, filter):
    if filter.strip() == '':
        return query
    tokens = sqlparse.parse(query)[0].tokens
    no_where = True
    for t in tokens:
        if type(t) == sqlparse.sql.Where:
            t.value = f'{t} and {filter} '
            no_where = False
        elif type(t) == sqlparse.sql.Token and t.normalized in ('GROUP', 'ORDER', 'LIMIT'):
            if no_where:
                t.value = f'where {filter} {t}'
            no_where = False
    if no_where:
        return f'{query} where {filter}'
    else:
        return ''.join([str(tt.value) for tt in tokens])

def test_MemSQL(a, b):
    import sys
    if ctx.get_xpu_id() != 1:
        {''}
        return
    try:
        # db = g._database = database.Connection(host='127.0.0.1', port=3300, user='root')
        # conn = database.connect(host="10.11.50.13", port=3300, user="root", password="xcalar")
        # conn = database.connect(host="10.11.50.9", port=3300, user="root", password="xcalar")
        conn = database.connect(host="127.0.0.1", port=3300, user="root", password="xcalar")
        conn.select_db('test')
        time_start = time.time()
        out = conn.query(test_sql) # limit 1000000
        time_end = time.time()
        logger.info(f'MemSQL ({ctx.get_xpu_id()}), executed qry: time:{time_end - time_start}')
        for z in out:
            yield dict(z)

        logger.info(f'MemSQL ({ctx.get_xpu_id()}), loaded table: time:{time.time() - time_end}')
                #for z in out:
        #    yield {**z, 'xpu':ctx.get_xpu_id()}
        #yield {'xpu':ctx.get_xpu_id(), 'output':f'{conn.query("show databases")}'}
    except Exception as e:
        logger.info(f'MemSQL error: str(e)')
        yield {'xpu':ctx.get_xpu_id(), 'sys':f'{sys.executable}', 'error':f'{str(e)}'} 

def utMemSQL(a, b, host, port, uid, psw, dbname, key):
    params = json.loads(kvstore.lookup(key))
    xpumap = params['bins']
    logger.info(f'MemSQL XPU MAP: {xpumap}')
    workitem = xpumap.get(str(ctx.get_xpu_id()), None)
    if workitem:
        yield {'name':'MemSQL'}
        query = params['query']
        # logger.info(f'MemSQL {ctx.get_xpu_id()} Workitem {workitem}')
        yield from loadMemSQL(host, port, uid, psw, dbname, query, workitem)
        
def loadMemSQL(host, port, uid, psw, dbname, query, workitem=''):  #key
    conn = None
    try:
        conn = database.connect(host=host, port=port, user=uid, password=psw)
        conn.select_db(dbname)
        if len(workitem):
            query = add_filter(query, workitem)

        time_start = time.time()
        out = conn.query(query)
        time_interval = time.time() - time_start
        logger.info(f'MemSQL Profile: xpu:{ctx.get_xpu_id()} | time:{time_interval} | {query}')
        for z in out:
            # yield {**z, 'xpu':ctx.get_xpu_id()}
            yield dict(z)
        #yield {'xpu':ctx.get_xpu_id(), 'output':f'{conn.query("show databases")}'}
    finally:
        if conn:
            conn.close()
        
        
        
        
        
        
def test_MySQL(a, b):  #key
    xpumap = {0:"1=1"}
    workitem = xpumap.get(ctx.get_xpu_id(), None)
    if workitem:
        cfg = {"host":"127.0.0.1", "port":"3300", "uid":"root", "psw":"xcalar","dbname":"test"}
        # query = "select * from pet"
        # query = "select * from TaxLots_Cost"
        host = cfg["host"]
        port = int(cfg["port"])
        dbname = cfg["dbname"]
        uid = cfg["uid"]
        psw = cfg["psw"]
        yield from loadMySQL(host, port, uid, psw, dbname, test_sql, workitem)

def utMySQL(a, b, host, port, uid, psw, dbname, key):
    params = json.loads(kvstore.lookup(key))
    xpumap = params['bins']
    logger.info(f'MySQL XPU MAP: {xpumap}')
    workitem = xpumap.get(str(ctx.get_xpu_id()), None)
    if workitem:
        query = params['query']
        # logger.info(f'MemSQL {ctx.get_xpu_id()} Workitem {workitem}')
        yield from loadMySQL(host, port, uid, psw, dbname, query, workitem)
    
def loadMySQL(host, port, uid, psw, dbname, query, workitem=''):  #key
    
    cursor = None
    connection = None
    try:
        connection = MySQLdb.connect(host=host, user=uid, port=int(port), passwd=psw, db=dbname, cursorclass = MySQLdb.cursors.SSCursor)

        if len(workitem):
            query = add_filter(query, workitem)
        
        time_start = time.time()
        
        OFFSET = 0
        LIMIT = 1000000
        has_records = True
        cursor = connection.cursor()
        while has_records:
            limit_query = f'{query} limit {LIMIT} offset {OFFSET}'
            # logger.info (limit_query)
            offset_start = time.time()
            cursor.execute(limit_query)
            columnNames = [x[0] for x in cursor.description]
            # time_interval = time.time() - time_start
            # yield {'log':f'xpu:{ctx.get_xpu_id()} | time:{time_interval} | {query}'}
            time_end = time.time()
            # logger.info(f'MySQL Profile: xpu:{ctx.get_xpu_id()} | time:{time_interval} | {query}')

            logger.info(f'MySQL Profile: ({ctx.get_xpu_id()}, {OFFSET}), executed qry: time:{time_end - offset_start}')

            has_records = False
            OFFSET += LIMIT
            while True:
                #_start = time.time()
                rows = cursor.fetchmany(100)
                #logger.info(f'MySQL Roundtrip: time:{time.time() - _start}')
                #_start2 = time.time() 
                if not rows:
                    break
                records = [dict(zip(columnNames, r)) for r in rows]
                yield from records
                has_records = True
                #logger.info(f'MySQL table_records: time:{time.time() - _start2}')

            logger.info(f'MySQL Profile: ({ctx.get_xpu_id()}, {OFFSET}), loaded table: time:{time.time() - time_end}')
        logger.info(f'MySQL Total: ({ctx.get_xpu_id()}), loaded table: time:{time.time() - time_start}')
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()