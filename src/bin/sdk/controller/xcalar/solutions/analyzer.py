from xcalar.solutions.query_builder import QueryBuilder
import pandas as pd
import sqlparse
import re
from tabulate import tabulate
from structlog import get_logger

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

logger = get_logger(__name__)


class Analyzer:
    def __init__(self, dispatcher, tableMap=None):
        self.dispatcher = dispatcher
        if not tableMap:
            tableMap = {}
            for t in dispatcher.session.list_tables():
                if (t.name.startswith('ut_')):
                    # parts = t.name[3:].split('1[0-9][0-9][0-9][0-9][0-9]+')
                    parts = re.split('[0-9]{10}#', t.name[3:])
                    tableMap[parts[0]] = max(t.name, tableMap.get(
                        parts[0], ''))
        self.tableMap = tableMap

    def execute(self, qb, params=None, schema={}, desc='', forced_name=None):
        _isdebug = self.dispatcher.isDebug
        self.dispatcher.isDebug = False
        p_table = self.dispatcher.execute(
            qb,
            params=params,
            schema=schema,
            desc=desc,
            forced_name=forced_name,
            prefix='analyzer')
        self.dispatcher.isDebug = _isdebug
        return p_table

    def dropTable(self, table):
        table = self.getSessionTable(table)
        self.dispatcher.dropTable(table)

    def describeTable(self, t_name, www):
        _tbl = self.dispatcher.get_table(t_name)
        meta = _tbl._get_meta()
        if t_name == www:
            tp = 'w'
        elif www and t_name > www:
            tp = 'f'
        else:
            tp = 'g'
        return {
            'id': t_name,
            'size': meta.total_size_in_bytes,
            'skew': meta.node_skew(),
            'rows': meta.total_records_count,
            'is_pinned': meta.pinned,
            'type': tp
        }

    def getTables(self, pattern='*'):
        # ts = self.dispatcher.session.list_tables(pattern)
        pattern = f'ut_{pattern}*'
        ts_names = [
            t.name for t in self.dispatcher.session.list_tables(pattern)
        ]
        ts_names.sort()
        out = []
        for tm in self.tableMap:
            tsns = [
                self.describeTable(ts, self.tableMap[tm]) for ts in ts_names
                if ts.startswith(f'ut_{tm}')
                and tm == re.split('[0-9]{10}#', ts[3:])[0]
            ]    # fixme: make more effecient
            if len(tsns):
                out.append({'name': tm, 'tables': tsns})
        return out

    def listTables(self):
        for t in self.tableMap:
            print(t, self.tableMap[t])

    def getSessionTable(self, table):
        if type(table) == str:
            table_name = self.tableMap.get(table, table)
            return self.dispatcher.get_table(table_name)
        else:
            return table

    def preFilter(self, table, pre_filter='eq(1,1)'):
        if pre_filter != 'eq(1,1)':
            qb = QueryBuilder(table.schema)
            qb.XcalarApiSynthesize(table_in=table.name, sameSession=False) \
                .XcalarApiFilter(pre_filter) \
                .XcalarApiSynthesize(columns=qb.synthesizeColumns())
            return self.execute(qb, schema=table.schema)
        else:
            return table

    def toPandasDf(self, table, n=1000, pre_filter='eq(1,1)'):
        records = self.getRecords(table, n, pre_filter)
        return pd.DataFrame.from_dict(records)

    def getRecords(self, table, n=3, pre_filter='eq(1,1)'):
        table = self.getSessionTable(table)
        table = self.preFilter(table, pre_filter)
        n = min(n, table.record_count())
        records = table.records(start_row=0, num_rows=n)
        # needs to be tested: will records generator live after table is dropped?
        # if pre_filter != 'eq(1,1)':
        #     table.drop(delete_completely=True)
        return records

    def show_table_or_dataset(self, table_or_dataset, columns, start,
                              num_rows):
        data = []
        for row in table_or_dataset.records(
                start_row=start, num_rows=num_rows):
            r_data = []
            for col in columns:
                if col in row:
                    r_data.append(row[col])
                else:
                    r_data.append(None)
            data.append(tuple(r_data))
        print(
            tabulate(
                pd.DataFrame.from_records(data, columns=columns),
                showindex=range(start, start + len(data)),
                headers='keys',
                tablefmt='psql',
            ))

    def profile(self, table_name, col_name, pre_filter='eq(1,1)', n=1000):
        table_name = self.tableMap.get(table_name, table_name)
        table_schema = self.dispatcher.get_table(table_name).schema
        col_type = reverseMap.get(table_schema[col_name], 'unknown')
        schema = {col_name: col_type, 'count': 'int'}
        qb = QueryBuilder(schema)
        qb.XcalarApiSynthesize(table_in='<table>', sameSession=False) \
            .XcalarApiFilter(pre_filter) \
            .XcalarApiGroupBy({'count': 'count(1)'}, group_on=[col_name]) \
            .XcalarApiSort({'count': 'Descending'}) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns()) \
            .getTempTableName()
        params = {'table': table_name}
        p_table = self.execute(qb, params=params, schema=schema)
        pdf = self.toPandasDf(p_table, n)
        self.dropTable(p_table)
        return pdf

    def rename(self, src_name, target_name):
        src = self.dispatcher.get_table(src_name)
        qb = QueryBuilder(schema=src.schema)
        qb.XcalarApiSynthesize(
            table_in=src_name,
            columns=qb.synthesizeColumns(),
            sameSession=False)
        out_table = self.execute(
            qb, schema=src.schema, forced_name=target_name)
        return out_table

    def executeSql(self, query, n=100):
        def renameTables(t):
            if hasattr(t, 'tokens'):
                if type(t) == sqlparse.sql.Identifier and len(t.tokens) == 1:
                    ts = t.tokens
                    token_val = re.sub(r'__(offsets|uber|info)', r'$\1',
                                       ts[0].value)
                    if token_val in self.tableMap:
                        ts[0] = f'`{self.tableMap[token_val]}`'
                    return ''.join([str(x) for x in ts])
                else:
                    return ''.join([renameTables(tt) for tt in t.tokens])
            else:
                return t.value

        query = re.sub(r'\$(offsets|uber|info)([\b\s\W]|$)', r'__\1\2',
                       query)    # sqlparse does not understand $
        tokens = sqlparse.parse(query)[0].tokens
        new_sql = ''.join([renameTables(t) for t in tokens])

        logger.info(f'sql> {new_sql}')
        tbl = self.dispatcher.session.execute_sql(
            new_sql, self.dispatcher.nextTable(prefix='sql'))
        pdf = self.toPandasDf(tbl, n)
        self.dropTable(tbl)
        return pdf
