import json
import uuid
typeMap = {
    'int': 'DfInt64',
    'integer': 'DfInt64',
    'string': 'DfString',
    'float': 'DfFloat64',
    'timestamp': 'DfTimespec',
    'date': 'DfTimespec',
    'money': 'DfMoney',
    'DfInt64': 'DfInt64',
    'DfString': 'DfString',
    'DfFloat64': 'DfFloat64',
    'DfTimespec': 'DfTimespec',
    'DfMoney': 'DfMoney',
    'DfUnknown': 'DfUnknown'
}


class QueryBuilder:
    def __init__(self, schema, fatPtr='fatPtr', metaSchema={}):
        self.schema = schema
        self.metaSchema = metaSchema
        self.dsetSeed = 'orchestrator'
        self.fatPtr = fatPtr
        self.query = []
        self.counter = 1
        self.uniqueIdentifier = uuid.uuid4().hex

    def evalFromDict(self, excpressions: dict):
        return [{
            'evalString': excpressions[x],
            'newField': x
        } for x in excpressions]

    def append(self, op):
        if type(op) is list:
            self.query = self.query + op
        else:
            self.query.append(op)
        return self

    def synthesizeColumns(self,
                          fromFP=False,
                          recordField=None,
                          dest_prefix='',
                          aSchema=None,
                          upperCase=False):
        if aSchema is None:
            schema = self.schema
            metaColumns = self.synthesizeColumns(
                aSchema=self.metaSchema, fromFP=fromFP, upperCase=upperCase)
        else:
            schema = aSchema
            metaColumns = []
        recordField = '{}.'.format(
            recordField) if recordField is not None else ''
        fatPtr = '{}::'.format(self.fatPtr) if fromFP else ''
        cc = metaColumns + \
            [{'sourceColumn': '{}{}{}'.format(fatPtr, recordField, c),
              'destColumn': '{}{}'.format(dest_prefix, (c.upper() if upperCase else c)),
              'columnType': typeMap[schema[c]]} for c in schema]
        if upperCase and aSchema is None:
            self.schema = {c.upper(): schema[c] for c in schema}
        return cc

    def getTempTableName(self, next=False, table_in=None):
        if table_in:
            return table_in
        if next:
            self.counter += 1
        return 'qb_{}_#{}'.format(self.uniqueIdentifier, self.counter)

    def getQuery(self, out_table='XcalarApiSynthesize_out#1'):
        query = self.query.copy()
        if query[-1]['operation'] == 'XcalarApiExport':
            query[-2]['args']['dest'] = out_table
            query[-1]['args']['source'] = out_table
            query[-1]['args']['dest'] = f'.XcalarLRQExport.{out_table}'
        else:
            out_table = 'XcalarApiSynthesize_out_{}#1'.format(
                self.uniqueIdentifier)
            query[-1]['args']['dest'] = out_table
        return query

    def XcalarApiSynthesize(self, table_in=None, columns=[], sameSession=True):
        op = {
            'operation': 'XcalarApiSynthesize',
            'args': {
                'source': self.getTempTableName(table_in=table_in),
                'dest': self.getTempTableName(next=True),
                'columns': columns,
                'sameSession': sameSession,
                'numColumns': len(columns)
            }
        }
        self.query.append(op)
        return self

    def XcalarApiFilter(self, evalString, table_in=None):
        if evalString == 'eq(1,1)':
            return self
        op = {
            'operation': 'XcalarApiFilter',
            'args': {
                'source': self.getTempTableName(table_in=table_in),
                'dest': self.getTempTableName(next=True),
                'eval': [{
                    'evalString': evalString,
                    'newField': None
                }]
            }
        }
        self.query.append(op)
        return self

    def XcalarApiUnion(self, table2, table_in=None, unionColumns=None):
        if not unionColumns:
            unionColumns = self.synthesizeColumns()
        if table_in is None:
            table_in = self.getTempTableName()
        self.query.append({
            'operation': 'XcalarApiUnion',
            'args': {
                'source': [table_in, table2],
                'dest': self.getTempTableName(next=True),
                'dedup': False,
                'columns': [unionColumns, unionColumns],
                'unionType': 'unionStandard',
                'key': []
            }
        })
        return self

    def _groupBy(self, aggrs, group_on, table_in=None):
        self.append({
            'operation': 'XcalarApiGroupBy',
            'args': {
                'source': self.getTempTableName(table_in=table_in),
                'dest': self.getTempTableName(next=True),
                'eval': self.evalFromDict(aggrs),
                'newKeyField': group_on[0].split('::')[-1],
                'includeSample': False,
                'icv': False,
                'groupAll': False
            }
        })
        return self

    def XcalarApiGroupBy(self, aggrs, group_on, table_in=None):
        self.append({
            'operation': 'XcalarApiIndex',
            'args': {
                'source':
                    self.getTempTableName(table_in=table_in),
                'dest':
                    self.getTempTableName(next=True),
                'key': [{
                    'name':
                        g,
                    'type':
                        typeMap[self.schema.get(
                            g.split('::')[-1], 'DfUnknown')],
                    'keyFieldName':
                        g.split('::')[-1],
                    'ordering':
                        'Unordered'
                } for g in group_on],
                'prefix':
                    '',
                'dhtName':
                    '',
                'delaySort':
                    False,
                'broadcast':
                    False
            }
        })._groupBy(aggrs, group_on)
        return self

    def XcalarApiNotExists(self, table2, pks, table_in=None):
        index1 = self.XcalarApiIndex(keys=pks, table_in=self.getTempTableName(table_in=table_in)) \
            .getTempTableName()
        index2 = self.XcalarApiIndex(
            keys=pks, table_in=table2).getTempTableName()
        self.append({
            'operation': 'XcalarApiJoin',
            'args': {
                'source': [index1, index2],
                'dest':
                    self.getTempTableName(next=True),
                'joinType':
                    'leftAntiJoin',
                'columns': [
                    self.synthesizeColumns(),
                    self.synthesizeColumns(aSchema=pks, dest_prefix='dummy_')
                ],
                'evalString':
                    '',
                'keepAllColumns':
                    False,
                'nullSafe':
                    False,
                'key': [list(pks.keys()), list(pks.keys())]
            }
        })
        return self

    def XcalarApiUpsert(self, pks, table_in=None, watermark_id='rn1'):
        groupbys = [k.split('::')[-1] for k in pks]
        tbl_index = self.XcalarApiIndex(
            keys=pks, table_in=table_in).getTempTableName()
        self._groupBy(aggrs={f'{watermark_id}_max': f'max({watermark_id})'}, group_on=groupbys) \
            .append({
                'operation': 'XcalarApiJoin',
                'args': {
                    'source': [
                        tbl_index,
                        self.getTempTableName()
                    ],
                    'dest': self.getTempTableName(next=True),
                    'joinType': 'innerJoin',
                    'columns': [
                        self.synthesizeColumns()
                        + self.synthesizeColumns(aSchema={watermark_id: 'DfInt64'}),
                        self.synthesizeColumns(aSchema={f'{watermark_id}_max': 'DfInt64'})
                    ],
                    'evalString': '',
                    'keepAllColumns': True,
                    'nullSafe': True,
                    'key': [groupbys, groupbys]
                }
            }) \
            .XcalarApiFilter(f'eqNonNull({watermark_id}_max,{watermark_id})')
        return self

    def XcalarApiTableCream(self, pks, table_in=None, watermark_id='batch_id'):
        groupbys = [k.split('::')[-1] for k in pks]
        sortkeys = {k: 'Ascending' for k in pks}
        sorted_tbl = self.XcalarApiSort(table_in=table_in,
                                        order_by={**sortkeys, watermark_id: 'Descending'})  \
            .XcalarApiRowNum('rn1') \
            .XcalarApiIndex(keys=pks).getTempTableName()

        self._groupBy(aggrs={'rn1_min': 'min(rn1)'}, group_on=groupbys) \
            .append({
                'operation': 'XcalarApiJoin',
                'args': {
                    'source': [
                        sorted_tbl,
                        self.getTempTableName()
                    ],
                    'dest': self.getTempTableName(next=True),
                    'joinType': 'innerJoin',
                    'columns': [
                        self.synthesizeColumns()
                        + self.synthesizeColumns(aSchema={'rn1': 'DfInt64'}),
                        self.synthesizeColumns(aSchema={'rn1_min': 'DfInt64'})
                    ],
                    'evalString': '',
                    'keepAllColumns': True,
                    'nullSafe': True,
                    'key': [groupbys, groupbys]
                }
            }) \
            .XcalarApiMap({'RN': 'addInteger(subInteger(rn1,rn1_min),1)'}) \
            .XcalarApiFilter('eqNonNull(RN,1)')
        return self

    def XcalarApiMap(self, evalMap={}):
        if evalMap == {}:
            return self
        self.query.append({
            'operation': 'XcalarApiMap',
            'args': {
                'source': self.getTempTableName(),
                'dest': self.getTempTableName(next=True),
                'eval': self.evalFromDict(evalMap),
                'icv': False
            }
        })
        return self

    def XcalarApiForceCast(self, doit=True, aSchema=None):
        if not doit:
            return self
        schema = self.schema if aSchema is None else aSchema
        table_in = self.getTempTableName()
        self.query.append({
            'operation': 'XcalarApiMap',
            'args': {
                'source':
                    table_in,
                'dest':
                    self.getTempTableName(next=True),
                'eval': [{
                    'evalString': '{}({})'.format(schema[c], c),
                    'newField': c
                } for c in schema],
                'icv':
                    False
            }
        })
        return self

    def XcalarApiUpperCast(self, doit=True, aSchema=None):
        if not doit:
            return self
        schema = self.schema if aSchema is None else aSchema
        table_in = self.getTempTableName()
        self.query.append({
            'operation': 'XcalarApiMap',
            'args': {
                'source':
                    table_in,
                'dest':
                    self.getTempTableName(next=True),
                'eval': [{
                    'evalString': '{}({})'.format(schema[c], c),
                    'newField': c.upper()
                } for c in schema],
                'icv':
                    False
            }
        })
        return self

    def XcalarApiForceTrim(self, doit=True, aSchema=None):
        if not doit:
            return self
        schema = self.schema if aSchema is None else aSchema
        self.query.append({
            'operation': 'XcalarApiMap',
            'args': {
                'source':
                    self.getTempTableName(),
                'dest':
                    self.getTempTableName(next=True),
                'eval': [{
                    'evalString': 'strip({})'.format(c),
                    'newField': c
                } for c in schema if schema[c] == 'string'],
                'icv':
                    False
            }
        })
        return self

    def XcalarApiDeskew(self, doit=True):
        if not doit:
            return self
        return self.XcalarApiIndex(
            keys={"xcalarRecordNum": "integer"}, ordering="Random")

    def XcalarApiRowNum(self, rn, table_in=None):
        self.query.append({
            'operation': 'XcalarApiGetRowNum',
            'args': {
                'source': self.getTempTableName(table_in=table_in),
                'dest': self.getTempTableName(next=True),
                'newField': rn
            }
        })
        return self

    def XcalarApiSort(self, order_by, table_in=None):
        self.query.append({
            'operation': 'XcalarApiIndex',
            'args': {
                'source':
                    self.getTempTableName(table_in=table_in),
                'dest':
                    self.getTempTableName(next=True),
                'key': [{
                    'name':
                        key,
                    'type':
                        typeMap[self.schema.get(
                            key.split('::')[-1], 'DfUnknown')],
                    'keyFieldName':
                        key.split('::')[-1],
                    'ordering':
                        order_by[key]
                } for key in order_by],
                'prefix':
                    '',
                'dhtName':
                    '',
                'delaySort':
                    False,
                'broadcast':
                    False
            }
        })
        return self

    # bulk load for regular tables
    def XcalarApiBulkLoad(self,
                          parserArg={},
                          targetName='generated',
                          parserFnName='dml:insert',
                          fileNamePattern='',
                          pk=None,
                          path='1',
                          recursive=False,
                          upperCase=False):
        dsetName = 'Optimized.{}.{}'.format(uuid.uuid4().hex, self.dsetSeed)
        schema = [{
            'sourceColumn': c,
            'destColumn': (c.upper() if upperCase else c),
            'columnType': typeMap[self.schema[c]]
        } for c in self.schema]
        if parserArg.get('parquetParser', None) == 'native':
            fileNamePattern = 're:.*parquet$'
        self.query.append({
            'operation': 'XcalarApiBulkLoad',
            'state': 'Unknown state',
            'args': {
                'dest': dsetName,
                'loadArgs': {
                    'sourceArgsList': [{
                        'targetName': targetName,
                        'path': p,
                        'fileNamePattern': fileNamePattern,
                        'recursive': recursive
                    } for p in path.split(',')],
                    'parseArgs': {
                        'parserFnName': parserFnName,
                        'parserArgJson': json.dumps(parserArg),
                        'fileNameFieldName': '',
                        'recordNumFieldName': '',
                        'allowFileErrors': False,
                        'allowRecordErrors': False,
                        'schema': schema
                    },
                    'size':
                        32737418240
                }
            }
        })
        if upperCase:
            self.schema = {c.upper(): self.schema[c] for c in self.schema}

        self.XcalarApiIndex(
            keys={'xcalarRecordNum': 'DfUnknown'},
            table_in='.XcalarDS.{}'.format(dsetName),
            prefix=self.fatPtr)
        if pk:
            if pk not in self.schema:
                self.XcalarApiRowNum(pk)
                self.schema = {**self.schema, pk: 'int'}
            self.XcalarApiIndex(keys={pk: 'int'}, prefix=self.fatPtr)

        return self

    # bulk load for imd tables
    def XcalarApiBulkLoadSort(self,
                              parserArg={},
                              targetName='generated',
                              parserFnName='dml:insert',
                              fileNamePattern='',
                              pks=None,
                              path='1',
                              recursive=False,
                              upperCase=False):
        dsetName = 'Optimized.{}.{}'.format(uuid.uuid4().hex, self.dsetSeed)
        if parserArg.get('schemaMode', None) == 'loadInput':
            schema = [{
                'sourceColumn': c,
                'destColumn': (c.upper() if upperCase else c),
                'columnType': typeMap[self.schema[c]]
            } for c in self.schema]
        elif parserArg.get('parquetParser', None) == 'native':
            schema = []
            fileNamePattern = 're:.*parquet$'
        else:
            schema = []
        self.query.append({
            'operation': 'XcalarApiBulkLoad',
            'state': 'Unknown state',
            'args': {
                'dest': dsetName,
                'loadArgs': {
                    'sourceArgsList': [{
                        'targetName': targetName,
                        'path': p,
                        'fileNamePattern': fileNamePattern,
                        'recursive': recursive
                    } for p in path.split(',')],
                    'parseArgs': {
                        'parserFnName': parserFnName,
                        'parserArgJson': json.dumps(parserArg),
                        'fileNameFieldName': '',
                        'recordNumFieldName': '',
                        'allowFileErrors': False,
                        'allowRecordErrors': False,
                        'schema': schema
                    },
                    'size':
                        32737418240
                }
            }
        })
        if upperCase:
            self.schema = {c.upper(): self.schema[c] for c in self.schema}

        if pks:
            self.query.append({
                'operation': 'XcalarApiIndex',
                'args': {
                    'source':
                        '.XcalarDS.{}'.format(dsetName),
                    'dest':
                        self.getTempTableName(next=True),
                    'key': [{
                        'name': key,
                        'type': 'DfUnknown',
                        'keyFieldName': key,
                        'ordering': 'PartialAscending'
                    } for key in pks],
                    'prefix':
                        self.fatPtr,
                    'dhtName':
                        '',
                    'delaySort':
                        False,
                    'broadcast':
                        False
                }
            })
        else:
            self.XcalarApiIndex(
                keys={'xcalarRecordNum': 'DfUnknown'},
                table_in='.XcalarDS.{}'.format(dsetName),
                prefix=self.fatPtr)
        return self

    def XcalarApiIndex(self,
                       keys,
                       table_in=None,
                       prefix='',
                       ordering='Unordered'):
        indexColumns = [{
            'name': c,
            'type': typeMap.get(keys[c], 'DfUnknown'),
            'keyFieldName': c if c != 'xcalarRecordNum' else '',
            'ordering': ordering
        } for c in keys]
        self.query.append({
            'operation': 'XcalarApiIndex',
            'args': {
                'source': self.getTempTableName(table_in=table_in),
                'dest': self.getTempTableName(next=True),
                'key': indexColumns,
                'prefix': prefix,
                'dhtName': '',
                'delaySort': False,
                'broadcast': False
            }
        })
        return self

    def XcalarApiExport(self, driverName, driverParams='{}'):
        # driverParams
        self.query.append({
            'operation': 'XcalarApiExport',
            'args': {
                'source':
                    self.getTempTableName(),
                'dest':
                    self.getTempTableName(next=True),
                'columns': [{
                    'columnName': c,
                    'headerName': c
                } for c in self.schema],
                'driverName':
                    driverName,
                'driverParams':
                    json.dumps(driverParams)
            }
        })
        return self
