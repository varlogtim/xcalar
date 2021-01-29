import json
from structlog import get_logger
import os
from pathlib import Path
from functools import reduce
from string import Template
import copy
import re
import inspect
import types
from types import SimpleNamespace
import numbers
from collections import OrderedDict
from xcalar.solutions.xcalar_client_utils import get_or_create_session

logger = get_logger(__name__)


class Universe:
    def initialize(self, dispatcher, universeAdapter, universeId, callbacks,
                   sdlcManager):
        """Creates a universe object from universe definition
        """
        self.dispatcher = dispatcher
        self.refiners = {}
        self.universe_id = universeId
        self.universeDef = universeAdapter.getUniverse(universeId)
        self.properties = SimpleNamespace(
            **self.universeDef['properties']
        ) if 'properties' in self.universeDef else SimpleNamespace()
        schema_names = []
        for key, value in {
                **self.universeDef['universe'],
                **self.universeDef.get('universeRefs', {})
        }.items():
            schema_names.append(value['schema'])
        self.schemas = universeAdapter.getSchemas(set(schema_names))
        self.params = self.universeDef['params']
        self.universe = self.universeDef['universe']
        self.sinkDefs = self.universeDef.get('sink_defs', {})
        self.sdlcManager = sdlcManager

        if not hasattr(self.properties, 'IMDThreadCount'):
            self.properties.IMDThreadCount = 1
        if not hasattr(self.properties, 'snapshotThreadCount'):
            self.properties.snapshotThreadCount = 2
        if not hasattr(self.properties, 'loadThreadCount'):
            self.properties.loadThreadCount = 1
        if not hasattr(self.properties, 'parallelOperations'):
            self.properties.parallelOperations = False
        if not hasattr(self.properties, 'sharedSession'):
            self.properties.sharedSession = False

        logger.debug(f'__UNIVERSE: {universeId}: {self.properties}')

        sourceDefs = self.universeDef['source_defs']
        params = self.params
        logger.debug('Creating universe from definition')
        appDefs = {
            self.universe[u]['app']['sourceDef']
            for u in self.universe if 'app' in self.universe[u]
        }
        self.appDefs = {u: sourceDefs[u] for u in appDefs}

        sdefs = [[
            self.universe[u].get('source', {}).get('sourceDef', None),
            self.universe[u].get('stream', {}).get('sourceDef', None)
        ] for u in self.universe]
        sourceDefs = {
            d: sourceDefs[d]
            for d in set(reduce(lambda x, y: (x + y), sdefs)) if d
        }
        metaSchema = {'BATCH_ID': 'int'}
        for d in sourceDefs:
            literals = sourceDefs[d].get('literals', [])
            for l in literals:
                luper = l.upper()
                if metaSchema.get(luper,
                                  literals[l]['type']) != literals[l]['type']:
                    raise Exception(
                        f'Inconsistent {luper} types in Universe: {literals[l]["type"]}, {metaSchema[luper]}'
                    )
                metaSchema[luper] = literals[l]['type']
        self.sourceDefs = sourceDefs

        for key, schema in self.schemas.items():
            columns = schema['columns']
            pks = schema.get('pks', schema.get('keys', []))
            for d in columns:
                if d in metaSchema:
                    raise Exception(
                        'A system name {} cannot be used in schema'.format(d))
            try:
                schema['pks'] = OrderedDict(
                    (d.upper(), columns[d]) for d in pks)
            except Exception:
                raise Exception(
                    'All primary keys should be defined in {} schema'.format(
                        key))
            columns = {**columns, **metaSchema}
            schema['source_columns'] = columns
            schema['columns'] = {s.upper(): columns[s] for s in columns}

        if len({self.universe[k]['stream'].get('path', k) for k in self.universe if 'stream' in self.universe[k]}) !=  \
                len({k for k in self.universe if 'stream' in self.universe[k]}):
            raise Exception(
                'Multiple universe tables cannot point to the same dataset')

        for u in self.universe:
            u_tbl = self.universe[u]
            if '_delta' in u:
                raise Exception(f'"_delta" not alowed in table name: {u}')
            u_tbl['metaschema'] = {'upper_cols': {}, 'source_cols': {}}
            src = u_tbl.get('source', None)
            sink = u_tbl.get('sink', None)
            if src:
                self.setMeta(src, callbacks, params)
                _path = src.get('path', None)
                if _path:
                    if '<' in _path:
                        param = _path[1:-1]
                        src['path'] = params[param]
            if 'stream' in u_tbl:
                u_tbl['stream']['prevKeys'] = None
                self.setMeta(u_tbl['stream'], callbacks, params)
                #  unfortunately IMD has the milestone id hardcoded, this creates confusion as all callumns are getting uppercased. TODO: more efficiency could be added here, request sent to eng
                if 'milestoneId' in u_tbl['stream']:
                    ms = u_tbl['stream'].pop('milestoneId')
                    u_tbl['stream']['sourceMilestoneId'] = ms['col'].upper()
                    u_tbl['metaschema']['source_cols'] = {
                        **u_tbl['metaschema']['source_cols'], ms['col']:
                            ms['type']
                    }
                    u_tbl['metaschema']['upper_cols'] = {
                        **u_tbl['metaschema']['upper_cols'], ms['col'].upper():
                            ms['type']
                    }
                if 'metaSchema' in u_tbl['stream']:
                    ms = u_tbl['stream'].pop('metaSchema')
                    ms_upper = {s.upper(): ms[s] for s in ms}
                    u_tbl['metaschema']['source_cols'] = {
                        **u_tbl['metaschema']['source_cols'],
                        **ms
                    }
                    u_tbl['metaschema']['upper_cols'] = {
                        **u_tbl['metaschema']['upper_cols'],
                        **ms_upper
                    }
            if 'app' in u_tbl:
                self.setApp(u_tbl['app'], sink, callbacks, u)
        self.orderedApps = self.linkApps()
        self.setImports(self.universeDef.get('universeRefs', {}))
        return self.universe

    def setImports(self, imports):
        self.linkedSessions = {}
        self.linkedTables = {}
        for key, imp in imports.items():
            universe_id = imp['from_universe_id']
            if universe_id not in self.linkedSessions:
                imp_session = get_or_create_session(
                    self.dispatcher.xcalarClient, universe_id)
                self.linkedSessions[universe_id] = imp_session.session_id
            self.linkedTables[key] = {
                **imp, 'session_id': self.linkedSessions[universe_id]
            }

    def setMeta(self, meta, callbacks, params):
        srcDef = self.sourceDefs[meta['sourceDef']]
        ls = srcDef.get('literals', {})
        if 'barrierCallBackName' in meta:
            meta['barrier'] = getattr(callbacks, meta['barrierCallBackName'])
        meta['forceTrim'] = meta.get('forceTrim', srcDef.get(
            'forceTrim', False))
        meta['forceCast'] = meta.get('forceCast', srcDef.get(
            'forceCast', False))
        meta['literals'] = {
            l.upper(): getattr(callbacks, ls[l]['eval'])
            for l in ls
        }
        _filter = meta.get('filter', None)
        if _filter:
            for p in params:
                _filter = _filter.replace('<{}>'.format(p), str(params[p]))
            meta['filter'] = _filter

    def setApp(self, meta, sink, callbacks, name):
        srcDef = self.appDefs[meta['sourceDef']]
        meta['dataflow'] = self.sdlcManager.checkout_dataflow(
            srcDef['args']['branchName'], meta['path'],
            f'ut_{name}_{self.universe_id}')
        tables = re.findall(r'"source": "<([_a-zA-Z0-9]*)>"',
                            meta['dataflow'].query_string)
        if len(tables) == 0:
            raise Exception(f'Application {name} has no inputs')
        meta['inputs'] = {t: None for t in tables}
        if 'barrierCallBackName' in meta:
            meta['decisionCallBack'] = getattr(callbacks,
                                               meta['barrierCallBackName'])
        meta['runtimeParamsCallBack'] = getattr(
            callbacks, meta['runtimeParamsCallBackName'])

        if sink:
            sinkDef = self.sinkDefs[sink['sinkDef']]
            exportDriverName = f'export_{name}_{self.universe_id}'
            try:
                export_udf_fname = self.sinkDefs['kafkaexport'][
                    'parserTemplateName']
                udf = self.dispatcher.xcalarClient.get_udf_module(
                    export_udf_fname)
                template = udf._get()
                code = """kafka_producer_props = {0}
{1}
                """.format(
                    json.dumps(sinkDef['args']['producerProperties']),
                    template)
            except Exception as e:
                logger.warning(e)
                outputUdfFName = os.path.dirname(
                    Path(__file__)) + '/udf/kafka_export_confluent_template.py'
                with open(outputUdfFName) as f:
                    code = '\n'.join(f.readlines())
            codeFinal = Template(code).substitute({
                'kafkaProducerProps':
                    f"{json.dumps(sinkDef['args']['producerProperties'])}",
                'exportDriverName':
                    exportDriverName
            })
            self.dispatcher.update_or_create_shared_udf(
                exportDriverName, codeFinal, unique_name=False)
            topic_partition, dataset_name = sink['path'].split('/')
            if 'max_headers' in sink:
                max_headers = sink['max_headers']
            else:
                max_headers = []
            if ':' in topic_partition:
                topic, partition = topic_partition.split(':')
                partition = partition
            else:
                topic = topic_partition
                partition = 0
            meta['kafka_export_driver'] = exportDriverName
            meta['kafka_export_params'] = {
                'avsc_fname':
                    sinkDef['args']['avscFname'],
                'kafka_params':
                    json.dumps({
                        'batch_size': sinkDef['args']['batchSize'],
                        'dataset_name': dataset_name
                    }),
                'kafka_topic':
                    topic,
                'target':
                    'Default Shared Root',
                'partition':
                    str(partition),
                "max_headers":
                    json.dumps(max_headers)
            }

    def linkApps(self):
        def findNext(_dlist, xdict):
            if xdict == {}:
                return _dlist
            else:
                _next = [d for d in xdict if len(xdict[d]) == 0]
                _xdict = {
                    dl: {d
                         for d in xdict[dl] if d not in _next}
                    for dl in xdict if len(xdict[dl]) != 0
                }
                if len(xdict) == len(_xdict):
                    raise Exception(
                        f'Circular reference: applciations, {_xdict}')
                # print(_next, _xdict)
                return findNext(_dlist + _next, _xdict)

        apps = self.getApps()
        dlist = {}
        for a in apps:
            dlist[a] = {
                i
                for i in apps[a]['inputs'] if i.split('_delta')[0] in apps
            }
        return findNext([], dlist)

    def getApps(self):
        return {
            k: self.universe[k]['app']
            for k in self.universe if 'app' in self.universe[k]
        }

    def getMeta(self, tbl, source_type):
        if source_type in tbl:
            return {
                'sourceDef': self.sourceDefs[tbl[source_type]['sourceDef']],
                'meta': tbl[source_type]
            }

    def getApp(self, tbl):
        if 'app' in tbl:
            return {
                'sourceDef': self.appDefs[tbl['app']['sourceDef']],
                'meta': tbl['app']
            }

    def getSchema(self, tbl):
        schema = self.schemas[tbl['schema']]
        metaschema = tbl.get('metaschema', {})
        return {
            'schema': {
                **schema['source_columns'],
                **metaschema.get('source_cols', {})
            },
            'schemaUpper': {
                **schema['columns'],
                **metaschema.get('upper_cols', {})
            },
            'pks': schema['pks']
        }

    def getSink(self, tbl):
        if 'sink' in tbl:
            return {
                'sinkDef': self.sinkDefs[tbl['sink']['sinkDef']],
                'meta': tbl['sink']
            }

    def getFlatUniverse(self):
        tables = {
            t: {
                **self.describeTable(t, 'all'),
                **self.getSchema(self.universe[t])
            }
            for t in self.universe
        }
        universeRefs = {**self.universeDef.get('universeRefs', {})}
        return {'tables': tables, 'universeRefs': universeRefs}

    def getTable(self, tble_name, source_type=None):
        if tble_name not in self.universe:
            return None
        tbl = self.universe[tble_name]
        if source_type:
            if source_type == 'all':
                u_object = {
                    'source': self.getMeta(tbl, 'source'),
                    'stream': self.getMeta(tbl, 'stream'),
                    'app': self.getApp(tbl),
                    'sink': self.getSink(tbl),
                    **self.getSchema(tbl)
                }
            elif source_type in tbl:
                u_object = {
                    **self.getMeta(tbl, source_type),
                    **self.getSchema(tbl)
                }
            else:
                return None
        else:
            u_object = self.getSchema(tbl)
        return u_object

    def describeTable(self, tble_name, source_type=None):
        def dictDescribe(dd):
            out = {}
            if dd is None:
                return dd
            for d in dd:
                if type(dd[d]) == dict:
                    v = dictDescribe(dd[d])
                elif isinstance(dd[d], types.FunctionType):
                    v = ''.join(inspect.getsource(dd[d]))
                elif isinstance(dd[d], numbers.Number):
                    v = dd[d]
                else:
                    v = str(dd[d])
                out[d] = v
            return out

        tbl = self.universe[tble_name]
        if not source_type:
            source_type = 'all'
        elif source_type not in ['source', 'stream', 'app', 'sink', 'all']:
            raise Exception(f'Unknown source type: {source_type}')
        desc = {}
        if source_type in ['all', 'source']:
            desc['source'] = dictDescribe(self.getMeta(tbl, 'source'))
        if source_type in ['all', 'stream']:
            desc['stream'] = dictDescribe(self.getMeta(tbl, 'stream'))
        if source_type in ['all', 'app']:
            desc['app'] = dictDescribe(self.getApp(tbl))
        if source_type in ['all', 'sink']:
            desc['sink'] = dictDescribe(self.getSink(tbl))
        return desc

    def removeRefiners(self):
        logger.debug('Removing previous refiners')
        previous_refiners = copy.deepcopy(self.refiners)
        self.refiners.clear()
        return previous_refiners

    def registerRefiner(self, refiner, orchestrator):
        if refiner.name in self.refiners:
            raise Exception('refiner {} already exists'.format(refiner.name))
        self.refiners[refiner.name] = refiner
        refiner.orchestrator = orchestrator

    def getDeltaIntervalInSeconds(self):
        key = 'deltaIntervalInSeconds'
        if key in self.properties.__dict__:
            return self.properties.deltaIntervalInSeconds
        return 5

    def getBatchRetentionSeconds(self):
        key = 'batchRetentionSeconds'
        if key in self.properties.__dict__:
            return self.properties.batchRetentionSeconds
        return 5000

    def getMinimumSleepIntervalInSeconds(self):
        key = 'minimumSleepIntervalInSeconds'
        if key in self.properties.__dict__:
            return self.properties.minimumSleepIntervalInSeconds
        return 5

    def getSnapshotDataTargetName(self):
        key = 'snapshotDataTargetName'
        if key in self.properties.__dict__:
            return self.properties.snapshotDataTargetName
        return 'snapshot'

    def getSnapshotMaxRetentionNumber(self):
        key = 'snapshotMaxRetentionNumber'
        if key in self.properties.__dict__:
            return self.properties.snapshotMaxRetentionNumber
        return None

    def getSnapshotMaxRetentionPeriodSeconds(self):
        key = 'snapshotMaxRetentionPeriodSeconds'
        if key in self.properties.__dict__:
            return self.properties.snapshotMaxRetentionPeriodSeconds
        return None

    def getDistributerProps(self):
        key = 'distributer'
        if key in self.properties.__dict__:
            return self.properties.distributer
        return {}
