from tabulate import tabulate
from colorama import Style
import pandas as pd
import sqlparse

from xcalar.solutions.tools.arg_parser import ArgParser
from xcalar.solutions.tools.connector import XShell_Connector
from xcalar.solutions.query_builder import QueryBuilder
import traceback

reverseMap = {
    'DfInt64': 'integer',
    'DfUInt32': 'integer',
    'DfFloat32': 'float',
    'DfFloat64': 'float',
    'DfString': 'string',
    'DfBoolean': 'boolean',
    'DfTimespec': 'timestamp',
    'DfMoney': 'money'
}

argp = ArgParser('analyze', [
    {
        'verbs': {
            'sql': 'sql statement'
        },
        'desc': 'SQL Statement to execute'
    },
    {
        'verbs': {
            'profile': 'table_name',
            'field': 'field_name',
            'n': 'number of records to print'
        },
        'desc': 'Display top n unique values in a table column'
    },
    {
        'verbs': {
            'exclude_na': 'table_name',
            'fields': 'field_names'
        },
        'desc': 'Exclude rows with nas in all fields and return new table name'
    },
    {
        'verbs': {
            'kmeans': 'table_name',
            'fields': 'field_names',
            'n': 'num_centroids',
        },
        'desc': 'k-means clustering'
    },
])

def dispatcher(line):
    usage = argp.get_usage()
    input_params = argp.get_params(line)
    xshell_connector = XShell_Connector()
    client = xshell_connector.get_client()
    dispatcher = xshell_connector.get_uber_dispatcher()
    # params = {
    #     'n': 20,
    #     'profile': None,
    #     'field': None,
    #     'sql': None,
    #     'table_map': {},
    #     'pre_filter':  'eq(1,1)'
    # }
    # for k, v in params.items():
    #     if k in input_params:
    #         params[k] = input_params[k]
    #     else:
    #         print(f'-> {k} not provided, setting to {v}')

    try:
        if 'sql' in input_params:
            params = argp.init_params(
                input_params, defaults={
                    'n': 20,
                    'sql': None,
                    'table_map': {}
                })
            executeSql(dispatcher, params)
        elif 'profile' in input_params:
            params = argp.init_params(
                input_params,
                defaults={
                    'n': 20,
                    'profile': None,
                    'field': None,
                    'profile': None,
                    'pre_filter': None,
                })
            profile(dispatcher, params)
        elif 'exclude_na' in input_params:
            params = argp.init_params(
                input_params,
                defaults={
                    'exclude_na': None,
                    'fields': None
                })
            excludena(dispatcher, params)
        elif 'kmeans' in input_params:
            params = argp.init_params(
                input_params,
                defaults={
                    'kmeans': None,
                    'fields': None,
                    'n': 5
                })
            kmeans(dispatcher, params)
        else:
            print(usage)
            return

    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return
    except Exception as err:
        print(str(err))
        traceback.print_tb(err.__traceback__)

def profile(dispatcher, params):
    table_name = params['profile']
    col_name = params['field']
    n = params['n']
    pre_filter = params['pre_filter']
    if '#' not in table_name:
        print('aliases not supported')
        return
    else:
        # get table id given label
        pass
    table_schema = dispatcher.get_table(table_name).schema
    col_type = reverseMap.get(table_schema[col_name], 'unknown')
    schema = {col_name: col_type, 'count': 'int'}
    qb = QueryBuilder(schema)
    # .XcalarApiFilter(pre_filter)
    qb.XcalarApiSynthesize(table_in=table_name, sameSession=False) \
        .XcalarApiGroupBy({'count': 'count(1)'}, group_on=[col_name]) \
        .XcalarApiSort({'count': 'Descending'}) \
        .XcalarApiSynthesize(columns=qb.synthesizeColumns()) \
        .getTempTableName()
    # p_table = self.execute(qb, params=params, schema=schema)
    p_table = dispatcher.execute(
        qb, schema=schema, desc='profiler', prefix='analyze')
    print_table(dispatcher, p_table, n)

def excludena(dispatcher, params):
    table_name = params['exclude_na']
    col_names = params['fields']
    table_schema = dispatcher.get_table(table_name).schema
    cols = set([f for f in table_schema])
    if col_names:
        cols = set(col_names).intersection(cols)
    cols = list(cols)
    nafilter = f'exists({cols[0]})'
    for c in cols[1:]:
        nafilter = f'and(exists({c}),{nafilter})'

    qb = QueryBuilder(table_schema)
    qb.XcalarApiSynthesize(table_in=table_name, sameSession=False) \
        .XcalarApiFilter(nafilter) \
        .XcalarApiSynthesize(columns=qb.synthesizeColumns()) \
        .getTempTableName()
    p_table = dispatcher.execute(
        qb, schema=table_schema, desc='profiler', prefix='table_name')
    # print_table(dispatcher, p_table, n)
    print(p_table.name)

def kmeans(dispatcher, params):
    table_name = params['kmeans']
    col_names = params['fields']
    n = int(params['n'])
    table_schema = dispatcher.get_table(table_name).schema
    cols = set([f for f,v in table_schema.items() if v in ['DfInt64','DfFloat64'] and f in col_names.split(',')])
    # if col_names:
    #     cols = set(col_names).intersection(cols)
    cols = list(cols)

    clist = ','.join([f'a.{c}' for c in cols])
    cost = '+'.join([f'(a.{c}-b.{c})*(a.{c}-b.{c})' for c in cols])
    avgs = ','.join ([f'avg(a.{c}) as {c}' for c in cols])
    normalize = ','.join ([f'{c} / MAX({c}) OVER () AS {c}' for c in cols])
    
    
    # sql_normalize = f'select ROW_NUMBER() as rn_id, normalize from <t>'
    # sql_init_centroids = f'select ROW_NUMBER() % {n} as centroid, {avgs} from <t> a group by 1'
    # sql_cost = f'a.*, b.centroid, {cost} as cost, ROW_NUMBER() OVER(PARTITION by a.rn_id ORDER BY ({cost}'
    sql_cost = f'a.*, b.centroid, ROW_NUMBER() OVER(PARTITION by a.rn_id ORDER BY ({cost}) as cost_rn from <t> a, <centroids> b'
    sql_filter = f'select a.* from cart a where cost_rn = 1'

#   prepare
    sql_normalize = f'select ROW_NUMBER() as rn_id, 0 as cid, {normalize} from <t>'
    data_t = execute_sql(dispatcher, sql_normalize, {'t':f'`{table_name}`'})
#   init centroids    
    sql_init_centroids = f'select rn_id % {n} as cid, {avgs} from <t> a group by 1'
    centr_t = execute_sql(dispatcher, sql_init_centroids, {'t':f'`{data_t}`'})
    for i in range(40):
        print('iteration ', i)
    #   asign centroids
        sql_iter = f"""with x as 
        (select {clist}, a.cid as old_cid, b.cid, ROW_NUMBER() OVER(PARTITION by a.rn_id ORDER BY ({cost})) as cost_rn, rn_id from <a> a, <b> b)
        select * from x where cost_rn = 1 """
        iter_t = execute_sql(dispatcher, sql_iter, {'a':f'`{data_t}`', 'b':f'`{centr_t}`'})

    #   check
        # sql_check = f'select count(*) as cnt from <a> a where old_cid <> cid'
        # centr_t = execute_sql(dispatcher, sql_check, {'a':f'`{iter_t}`'})
        # modified_centroids = getRecords(dispatcher, centr_t,1).__next__()['CNT']
        # print('modified centroids:', modified_centroids)
        # if modified_centroids < 1:
        #     print('done:', iter_t)
        #     break
        
    #   new centroids
        sql_centroids = f'select cid, {avgs}, sum(case when cid=old_cid then 0 else 1 end)/count(*) as changes from <t> a group by 1'
        centr_t = execute_sql(dispatcher, sql_centroids, {'t':f'`{iter_t}`'})
        # changes = 0
        # for r in getRecords(dispatcher, centr_t,10):
        #     print(r)
        #     changes += r['CHANGES']
        n_changes = sum([r['CHANGES'] for r in getRecords(dispatcher, centr_t,n)])/n
        print('% modified centroids:', n_changes)
        if n_changes < 0.001:
            break
        data_t = iter_t

#NYSE_20#v13756
def execute_sql(dispatcher, sql_query, params, pks=None, prefix='analyze_', drop_src_tables=False):
    for p, v in params.items():
        sql_query = sql_query.replace(f'<{p}>', f'{v}')

    # print(sql_query)
    tbl_name = dispatcher.session.execute_sql(
        sql_query=sql_query,
        result_table_name=dispatcher.nextTable(prefix=prefix),
        drop_src_tables=drop_src_tables)
    if pks:
        table = dispatcher.get_table(tbl_name)
        schema = table.schema
        qb = QueryBuilder(schema=schema)
        qb.XcalarApiSynthesize(table_in=tbl_name, columns=qb.synthesizeColumns(), sameSession=False) \
          .XcalarApiIndex(keys={pk: schema[pk] for pk in pks})
        tbl = dispatcher.execute(qb, desc='indexing', prefix=prefix)
        return tbl.name
    else:
        return tbl_name

def getRecords(dispatcher, table_name, n=3):
    table = dispatcher.get_table(table_name)
    n = min(n, table.record_count())
    records = table.records(start_row=0, num_rows=n)
    return records














    return sql_init_centroids


def executeSql(dispatcher, params):
    tableMap = params['table_map']
    query = params['sql']
    n = params['n']

    def renameTables(t):
        if hasattr(t, 'tokens'):
            if type(t) == sqlparse.sql.Identifier and len(t.tokens) == 1:
                ts = t.tokens
                # token_val = re.sub(r'__(offsets|uber|info)', r'$\1',
                #                     ts[0].value)
                token_val = ts[0].value
                if token_val in tableMap:
                    ts[0] = f'`{tableMap[token_val]}`'
                return ''.join([str(x) for x in ts])
            else:
                return ''.join([renameTables(tt) for tt in t.tokens])
        else:
            return t.value

    # query = re.sub(r'\$(offsets|uber|info)([\b\s\W]|$)', r'__\1\2',
    #                 query)    # sqlparse does not understand $
    tokens = sqlparse.parse(query)[0].tokens
    new_sql = ''.join([renameTables(t) for t in tokens])

    print(f'sql> {new_sql}')
    tbl = dispatcher.session.execute_sql(new_sql,
                                         dispatcher.nextTable(prefix='sql'))
    # pdf = self.toPandasDf(tbl, n)
    print_table(dispatcher, tbl, int(n))


def print_table(dispatcher, table_name, n=1000):
    table = dispatcher.get_table(table_name)
    n = min(n, table.record_count())
    records = table.records(start_row=0, num_rows=n)
    # return pd.DataFrame.from_dict(records)
    pdf = pd.DataFrame.from_dict(records)
    dispatcher.dropTable(table)
    print(tabulate(
        pdf,
        headers='keys',
        tablefmt='psql',
    ))


def load_ipython_extension(ipython, *args):
    argp.register_magic_function(ipython, dispatcher)


def unload_ipython_extension(ipython):
    pass
