from xcalar.solutions.analyzer import Analyzer
import json
from structlog import get_logger

logger = get_logger(__name__)
reverseMap = {
    'int': 'integer',
    'DfInt64': 'integer',
    'DfUInt32': 'integer',
    'DfFloat32': 'float',
    'DfFloat64': 'float',
    'DfString': 'string',
    'DfBoolean': 'boolean',
    'DfTimespec': 'timestamp',
    'DfMoney': 'money',
}
dag_prefix = 'dag_$$$$$$$$$$$$$$$$_999999999999'    # note: one digit less


class XDBridge:
    def __init__(self,
                 dispatcher=None,
                 analyzer=None,
                 refiner=None,
                 orchestrator=None):    # consistent universe
        if analyzer:
            logger.info('XD Bridge for analyzer')
            self.mode = 'analyzer'
            self.analyzer = analyzer
            self.dispatcher = analyzer.dispatcher
        elif dispatcher:
            logger.info('XD Bridge for all active session tables')
            self.mode = 'session'
            self.dispatcher = dispatcher
            self.analyzer = Analyzer(dispatcher)
        elif refiner:
            logger.info(f'XD Bridge for refiner {refiner}')
            self.mode = 'refiner'
            self.dispatcher = orchestrator.dispatcher
            self.analyzer = Analyzer(self.dispatcher, tableMap=refiner)
        elif orchestrator:
            logger.info('XD Bridge for the complete universe')
            self.mode = 'schema'
            self.orchestrator = orchestrator
            self.dispatcher = orchestrator.dispatcher
            self.analyzer = orchestrator.getAnalyzer()
        else:
            raise Exception('No argument provided')

    def __comment(self, i, j, t_name, schema, description):
        return [{
            'id':
                f'comment999_{i}',    # XD inconsistency: nodeId in clipboard, id in kvStore!!!
            'display': {
                'x': 10,
                'y': 10 + 70 * i,
                'width': 200,
                'height': 38
            },
            'text': description,
        }]

    def __linkIn(self, i, j, t_name, schema, description):
        return [{
            'type': 'link in',
            'subType': None,
            'display': {
                'x': (j + 1) * 200 + 20,
                'y': 10 + 70 * i
            },
            'description': description,
            'input': {
                'dataflowId': '',
                'linkOutName': '',
                'source': t_name
            },
            'state': 'Configured',
            'configured': True,
            'aggregates': [],
            'schema': [{
                'name': k,
                'type': schema[k]
            } for k in schema],
            'parents': [],
        # "nodeId": "dag_{}_{}".format(j, i)
            'id':
                f'{dag_prefix}_0_{i}',    # XD inconsistency: nodeId in clipboard, id in kvStore!!!
        }]

    def __noop(self, i, j):
        return [{
            'type': 'filter',
            'subType': None,
            'display': {
                'x': (j + 1) * 200 + 20,
                'y': 10 + 70 * i
            },
            'description': '',
            'input': {
                'evalString': 'eq(1, 1)'
            },
            'state': 'Configured',
            'configured': True,
            'aggregates': [],
            'parents': [f'{dag_prefix}_{j-1}_{i}'],
            'id': f'{dag_prefix}_{j}_{i}'
        }]

    def __linkOut(self, i, j, t_name, description=''):
        return [{
            'type': 'link out',
            'subType': None,
            'display': {
                'x': (j + 1) * 200 + 20,
                'y': 10 + 70 * i
            },
            'description': description,
            'input': {
                'name': t_name,
                'linkAfterExecution': False,
                'columns': []
            },
            'state': 'Configured',
            'configured': True,
            'aggregates': [],
            'parents': [f'{dag_prefix}_{j-1}_{i}'],
            'id':
                f'{dag_prefix}_{j}_{i}',    # XD inconsistency: nodeId in clipboard, id in kvStore!!!
        }]

    def get_session_map(self):
        session_map = {}
        for s in self.dispatcher.xcalarClient.list_sessions():
            session_map[s.session_id] = s
        return session_map

    def get_tbl(self, t_name, session_map):
        session_id = t_name.split('/')[2]
        session = session_map.get(session_id, '')
        _tbl = session.get_table(t_name.split('/')[3])
        return _tbl

    def getLinkins(self):
        nodes = []
        comments = []
        if self.mode == 'schema':
            rmap = self.analyzer.tableMap
            universe = self.orchestrator.flat_universe['tables']
            for u in universe:
                tbl = universe[u]
                schema = tbl['schemaUpper']
                # create empty tables if they do not exist
                if ('source' in tbl or 'app' in tbl) and not rmap.get(u, None):
                    _tbl = self.dispatcher.createEmptyTable(schema, prefix=u)
                    rmap[u] = _tbl.name
                if ('stream' in tbl
                        or 'app' in tbl) and not rmap.get(f'{u}_delta', None):
                    _tbl = self.dispatcher.createEmptyTable(
                        schema, prefix=f'{u}_delta')
                    rmap[f'{u}_delta'] = _tbl.name
            self.analyzer = Analyzer(self.dispatcher, rmap)
        tableMap = self.analyzer.tableMap
        tables = [k for k in tableMap if tableMap[k]]
        tables.sort()
        session_map = self.get_session_map()
        for i, t in enumerate(tables):
            t_name = tableMap[t]
            if '/' in t_name:
                _tbl = self.get_tbl(t_name, session_map)
            else:
                _tbl = self.dispatcher.session.get_table(t_name)
            # if not _tbl.is_pinned(): # this method is very slow, replacing with try/catch
            try:
                _tbl.pin()
            except Exception:
                pass
            schema = {
                k: reverseMap.get(_tbl.schema[k], 'unknown')
                for k in _tbl.schema
            }
            nodes += self.__linkIn(i, 0, t_name, schema,
                                   f'{t}, {_tbl.record_count()} records')
            comments += self.__comment(i, 0, t_name, schema,
                                       f'{t}, {_tbl.record_count()} records')
            nodes += self.__linkOut(i, 1, t)
        out = {
            'nodes': nodes,
            'comments': comments,
            'display': {
                'width': -1,
                'height': -1,
                'scale': 1
            },
            'operationTime': 0,
        }
        return out

    def getLinkinParams(self):
        nodes = []
        comments = []
        rmap = {}
        if self.mode == 'schema':
            universe = self.orchestrator.flat_universe['tables']
            for u in universe:
                tbl = universe[u]
                schema = tbl['schemaUpper']
                schema = {
                    k: reverseMap.get(schema[k], schema[k])
                    for k in schema
                }
                if 'source' in tbl or 'app' in tbl:
                    rmap[u] = schema
                if 'stream' in tbl or 'app' in tbl:
                    rmap[f'{u}_delta'] = schema
        else:
            tableMap = self.analyzer.tableMap
            session_map = self.get_session_map()
            tables = [k for k in tableMap if tableMap[k]]
            tables.sort()
            for i, t in enumerate(tables):
                table = tableMap[t]
                if '/' in table:
                    _tbl = self.get_tbl(table, session_map)
                else:
                    _tbl = self.dispatcher.session.get_table(table)
                rmap[t] = {
                    k: reverseMap.get(_tbl.schema[k], 'unknown')
                    for k in _tbl.schema
                }
        tables = [k for k in rmap if rmap[k]]
        tables.sort()
        for i, t in enumerate(tables):
            schema = rmap[t]
            nodes += self.__linkIn(i, 0, f'<{t}>', schema, f'<{t}>')
            comments += self.__comment(i, 0, f'<{t}>', schema, f'<{t}>')
            nodes += self.__noop(i, 1)
            nodes += self.__linkOut(i, 2, t)
        out = {
            'nodes': nodes,
            'comments': comments,
            'display': {
                'width': -1,
                'height': -1,
                'scale': 1
            },
            'operationTime': 0,
        }
        return out

    def updateSources(self, mode='params', input_dflow='Input Data'):
        dag_id = 'DF2_$$$$$$$$$$$$$$$$_9999999999999_1'
        kvstore = self.dispatcher.workbook.kvstore
        keys = kvstore.list(self.dispatcher.workbook._list_df_regex)
        for k in keys:
            dataflow = json.loads(kvstore.lookup(k))
            if dataflow['name'] == input_dflow or k == dag_id:
                logger.info(f'deleting {k}')
                kvstore.delete(k)
        if mode == 'params':
            logger.info('Getting parameterized linkins')
            dags = self.getLinkinParams()
        else:    # session: actual tables
            logger.info('Getting linkins pointing to tables')
            dags = self.getLinkins()
        dags = json.dumps({'name': input_dflow, 'id': dag_id, 'dag': dags})
        kvstore.add_or_replace(dag_id, dags, True)
