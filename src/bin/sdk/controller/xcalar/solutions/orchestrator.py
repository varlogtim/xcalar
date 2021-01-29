# orchestrator
from xcalar.solutions.query_builder import QueryBuilder
from xcalar.solutions.watermark import Watermark
from xcalar.solutions.analyzer import Analyzer
from xcalar.solutions.state_persister import StatePersister
from xcalar.solutions.snapshot import SnapshotManagement
from xcalar.solutions.offset_manager import OffsetManager
from xcalar.solutions.controller_flags import get_controller_flags
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.logging_config import LOGGING_CONFIG
import logging
import logging.config
import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars

import concurrent.futures
from datetime import datetime
from structlog import get_logger
import importlib
import os
import time
from dateutil.parser import parse
from xcalar.solutions.timer import TimerMsg
import copy
import random
import json

logger = get_logger(__name__)


class Orchestrator:
    info_schema = {
        'topic': 'string',
        'status': 'string',
        'xpu_id': 'int',
        'timestamp': 'timestamp',
        'batch_id': 'int',
        'msg': 'string'
    }
    info_pk_list = ['TOPIC', 'XPU_ID', 'BATCH_ID']

    def initialize(self,
                   dispatcher,
                   universe,
                   snapshot_data_target_name,
                   snapshot_max_retention_number,
                   snapshot_max_retention_period_seconds,
                   isTimetoPurge,
                   xpu_count,
                   connector=None,
                   batch_retention_seconds=1000,
                   pause_freq_seconds=30):    # seconds
        self.update(dispatcher, universe, snapshot_data_target_name,
                    snapshot_max_retention_number,
                    snapshot_max_retention_period_seconds, isTimetoPurge, xpu_count,
                    connector, batch_retention_seconds, pause_freq_seconds)
        self.reset_state()

    def reset_state(self):
        self.watermark = Watermark(_objects={})
        self.offset_manager = OffsetManager().initialize(
            self.dispatcher, self.uber_universe, self.isTimetoPurge, self.batch_retention_seconds)

    def update(self,
               dispatcher,
               universe,
               snapshot_data_target_name,
               snapshot_max_retention_number,
               snapshot_max_retention_period_seconds,
               isTimetoPurge,
               xpu_count,
               connector=None,
               batch_retention_seconds=1000,
               pause_freq_seconds=30):    # seconds
        self.xpu_count = xpu_count
        dispatcher.parallel_operations = universe.properties.parallelOperations
        dispatcher.shared_session = universe.properties.sharedSession
        self.dispatcher = dispatcher
        logger.debug(
            f'__SESSION: {dispatcher.session.name}: shared_session:{dispatcher.shared_session}, parallel_operations:{dispatcher.parallel_operations}'
        )
        self.uber_connector = connector
        self.fatPtr = dispatcher.fatPtr
        self.batch_retention_seconds = batch_retention_seconds
        self.uber_universe = universe

        self.flat_universe = universe.getFlatUniverse()
        self.snapshot_data_target_name = snapshot_data_target_name
        self.snapshot_max_retention_number = snapshot_max_retention_number
        self.snapshot_max_retention_period_seconds = snapshot_max_retention_period_seconds
        self.pause_freq_seconds = pause_freq_seconds
        self.isTimetoPurge = isTimetoPurge

    def dynaload(self, name):
        spec = importlib.util.find_spec(name)
        if spec is None:
            raise Exception(f'No {name} plugin found.')
        return spec.loader.load_module()

    def getAnalyzer(self):
        if self.watermark.mapObjects():
            return Analyzer(self.dispatcher, self.watermark.mapObjects())
        else:
            raise Exception(
                f'{self.dispatcher.session.name} Watermark is empty')

    def registerConnector(self, connector, xpuPartitionMap):
        connector.dispatcher = self.dispatcher
        connector.fatPtr = self.dispatcher.fatPtr
        connector.universe = self.uber_universe.universe
        connector.xpuPartitionMap = xpuPartitionMap
        connector.targetName = 'kafka-base'
        self.dispatcher.update_or_create_datatarget(connector.targetName,
                                                    'memory')
        self.uber_connector = connector

    def hydrateUniverseFromSource(self, reuse_dirty=False):
        logger.debug('__EVENT: UNIVERSE LOAD STARTING')
        # TBD: intitate info table instead of offsets, populate offsets after all done, include records number
        logger.info(f'reuse_dirty = {reuse_dirty} and watermark = {self.watermark.mapObjects()}')
        if reuse_dirty:
            self.offset_manager.startTransaction(watermark=self.watermark)
        else:
            self.offset_manager.startTransaction()
        watermark = self.offset_manager.watermark
        now = datetime.now().timestamp() * 1000
        schemaUpper = {
            s.upper(): self.info_schema[s]
            for s in self.info_schema
        }
        qb = self.dispatcher.insertFromJson({**schemaUpper, **{'BATCH_ID': 'int'}}, records=[{
            'TOPIC': '-',
            'STATUS': 'info',
            'XPU_ID': 1,
            'BATCH_ID': 1,
            'TIMESTAMP': now,
            'MSG': 'hydration from source'
        }], execute=False) \
            .XcalarApiSort({k: 'PartialAscending' for k in self.info_pk_list})
        table = self.dispatcher.execute(
            qb, desc='create info table', prefix='info')

        self.offset_manager.appendRecord(
            now, now, t_name='info', record_count=table.record_count())
        watermark.info = table.name
        a_list = Analyzer(self.dispatcher).tableMap.values()
        worker_args = []
        for t_name in self.uber_universe.universe:
            if reuse_dirty:
                t_id = watermark.getBase(t_name)
                if t_id in a_list:
                    if t_name not in [
                            o['dataset'] for o in self.offset_manager.offsets
                    ]:
                        rownum = self.dispatcher.get_table(t_id).record_count()
                        self.offset_manager.appendRecord(
                            now, now, t_name=t_name, record_count=rownum)
                    logger.info(f'Found existing materialized table: {t_name}')
                    continue
            worker_args.append((t_name, 'source', watermark))
        self.loadParallel(worker_args)
        logger.debug('__EVENT: UNIVERSE LOAD FINISHED')
        self.watermark = self.offset_manager.commitRecords()

    def loadParallel(self, worker_args):
        num_workers = min(self.uber_universe.properties.loadThreadCount,
                          len(worker_args))
        logger.debug(f'Running loading with {num_workers} threads')
        if num_workers > 0:
            with concurrent.futures.ThreadPoolExecutor(
                        max_workers=num_workers) as executor:
                    results = executor.map(
                        lambda args: self.loadFromSource(*args), worker_args)
                    # need to iterate for the results to raise the exception of child threads
                    for ret in results:
                        print(f"load result {ret}")

    def adjustUniverse(self, table_names, action):
        logger.debug('__EVENT: UNIVERSE ADJUSTMENT STARTING')
        logger.debug(f'Universe adjustment for: {table_names}')
        if action in ['add']:
            for table_name in table_names:
                if table_name in self.watermark.objects:
                    raise Exception(
                        f'Error: Could not {action} table {table_name}: already exists in the universe'
                    )
        if action in ['drop', 'replace']:
            for table_name in table_names:
                if table_name not in self.watermark.objects:
                    raise Exception(
                        f'Error: Could not {action} table {table_name}: it does exists in the universe'
                    )

        if action in ['drop']:
            for table_name in table_names:
                self.watermark.objects.pop(table_name)
        if action in ['add', 'replace']:
            self.offset_manager.startTransaction(self.watermark)
            watermark = self.offset_manager.watermark
            worker_args =[]
            for table_name in table_names:
                worker_args.append((table_name, 'source', watermark))
            self.loadParallel(worker_args)
            self.watermark = self.offset_manager.commitRecords(
                base_table=watermark.offsets)
        self.cleanLastBatch()
        logger.debug('__EVENT: UNIVERSE ADJUSTMENT FINISHED')

    def adjustOffsetTable(self, topic: str, partitions: str,
                          new_offset: int) -> None:
        logger.debug('__EVENT: OFFSETS ADJUSTMENT STARTING')
        self.offset_manager.startTransaction(self.watermark)
        watermark = self.offset_manager.watermark
        for partition in partitions.split(','):
            logger.info(
                f'Updating Offset table with - Topic: {topic}, Partition: {partition}, New Offset: {new_offset}'
            )
            now = datetime.now().timestamp() * 1000
            self.offset_manager.appendRecord(
                start_dt=now,
                end_dt=now,
                topic=topic,
                partition=partition,
                max_offset=new_offset)
        self.watermark = self.offset_manager.commitRecords(
            base_table=watermark.offsets)
        logger.debug('__EVENT: OFFSETS ADJUSTMENT FINISHED')

    def loadFromSource(self, t_name, source_type, watermark):
        def distribute_xpus(filters):
            num_cpus = self.xpu_count
            len_f = len(filters)
            cpus = range(num_cpus)
            if len_f > num_cpus:
                new_filters = [[]] * num_cpus
                random.shuffle(filters)
                for i, f in enumerate(filters):
                    new_filters[i % num_cpus] = new_filters[i % num_cpus] + [f]
                return dict(zip(cpus, new_filters))
            else:
                cpus = random.sample(cpus,len_f)
                return dict(zip(cpus, filters))
        logging.config.dictConfig(LOGGING_CONFIG)
        structlog.configure(
            processors=[
                merge_contextvars,
                structlog.processors.KeyValueRenderer()
            ],
            context_class=structlog.threadlocal.wrap_dict(dict),
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        clear_contextvars()
        bind_contextvars(universe_id=self.uber_universe.universe_id)
        tbl = self.uber_universe.getTable(t_name, source_type)
        if not tbl:
            watermark.setBase(t_name, None)
            return
        logger.debug('State: processing... {}'.format(t_name))
        source = tbl['meta']
        if 'barrier' in source:
            if not source['barrier'](watermark, watermark.low_batch_id):
                logger.debug('not yet reloading... {}'.format(t_name))
                return
        watermark.setDelta(t_name, None)
        start_dt = datetime.now().timestamp() * 1000
        sourceDef = tbl['sourceDef']
        literals = source.get('literals', {})
        paths = [source['path']] if type(
            source['path']) == str else source['path']
        schemaUpper = tbl['schemaUpper']
        literals = {
            literal: '{}({})'.format(schemaUpper[literal], literals[literal]())
            for literal in literals if literal != 'BATCH_ID'
        }
        literals = {'BATCH_ID': f'int({watermark.batch_id})', **literals}
        last_table = None
        for i, path in enumerate(paths):
            if 'basePath' in sourceDef:
                path = ','.join([
                    os.path.join(sourceDef['basePath'], p.strip())
                    for p in path.split(',')
                ])
            parseArgs = copy.deepcopy(sourceDef['parseArgs'])
            logger.debug(f"YYY  t_name: {t_name}, parseArgs: {source.get('parseArgs',{1:2})}")
            tble_src_args = copy.deepcopy(source.get('parseArgs',{}))
            for key, val in tble_src_args.items():
                 if key == 'key':
                    #  logger.debug(f'XXX1 bins: {val["bins"]}')
                     val["bins"] = distribute_xpus(val.get("bins",['']))
                     logger.debug(f'YYY bins: {val["bins"]}')
                     kvKey = self.dispatcher.send_2kvstore(json.dumps(val))
                     parseArgs[key] = kvKey
                 else:
                     parseArgs[key] = val
            # logger.debug(f'XXX t_name: {t_name}, parseArgs: {parseArgs}')
            qb = QueryBuilder(tbl['schema'])
            fromFP = True
            tb_name = qb.XcalarApiBulkLoad(parserArg=parseArgs,
                                           targetName=sourceDef['targetName'],
                                           parserFnName=sourceDef['parserFnName'],
                                           path=path,
                                           recursive=source['isFolder']) \
                .XcalarApiFilter(source['filter']) \
                .XcalarApiSynthesize(columns=qb.synthesizeColumns(upperCase=True, fromFP=fromFP)) \
                .XcalarApiDeskew(source.get('deskew', True)) \
                .XcalarApiForceCast(source.get('forceCast', False)) \
                .XcalarApiForceTrim(source.get('forceTrim', False)) \
                .XcalarApiMap(literals).getTempTableName()
            if last_table:
                qb.XcalarApiSynthesize(table_in=last_table.name, sameSession=False) \
                  .XcalarApiUnion(tb_name)
            if i == len(paths) - 1:
                qb.XcalarApiSort({k: 'PartialAscending' for k in tbl['pks']})
            next_table = self.dispatcher.execute(
                qb, prefix=f'{t_name}_base', pin_results=True)
            if last_table:
                self.dispatcher.dropTable(last_table)
            last_table = next_table
        self.offset_manager.appendRecord(
            start_dt,
            datetime.now().timestamp() * 1000,
            t_name=t_name,
            record_count=last_table.record_count())
        watermark.setBase(t_name, last_table.name)
        self.dispatcher.publish_table(last_table.name)

    def pullDeltas(self):
        logger.debug(f'__EVENT: CLEANING UP')
        self.cleanLastBatch()
        # _universe = self.uber_universe.universe
        # number_of_kafka_pulls = self.uber_universe.mergeQueueLen
        # watermark = self.watermark
        # for i in range(number_of_kafka_pulls):
        #     watermark = self.offset_manager.startTransaction(watermark,
        #                                         self.batch_retention_seconds)
        #     batch_id = self.offset_manager.getBatchId()
        #     logger.debug(f'__EVENT: BATCH START, batch id {batch_id}')
        #     offsets = self.offset_manager.getKafkaOffsets()
        #     uber_table, dsetName = self.uber_connector.nextBatch(batch_id, offsets)
        #     watermark = self.offset_manager.commitRecords(base_table=watermark.offsets)
        self.offset_manager.startTransaction(self.watermark,
                                             self.batch_retention_seconds)
        batch_id = self.offset_manager.getBatchId()
        logger.debug(f'__EVENT: BATCH START, batch id {batch_id}')
        watermark = self.offset_manager.watermark
        assert watermark.checkLatest()

        kafka_universe = [k for k in self.uber_universe.universe
            if 'stream' in self.uber_universe.universe[k]]
        if len(kafka_universe):
            #logger.debug(f'__EVENT: BATCH START, batch id {batch_id}')
            offsets = self.offset_manager.getKafkaOffsets()
            uber_table, dsetName = self.uber_connector.nextBatch(batch_id, offsets)
            logger.debug(f'__EVENT: IMD STARTING, batch id {batch_id}')


            if watermark.getCurrentDataset(
            ) is not None:    # workaround. remove when dset leak fixed in XCE
                watermark.addOldDataset(
                    watermark.curr_dataset
                )    # workaround. remove when dset leak fixed in XCE
            watermark.setCurrentDataset(
                dsetName)    # workaround. remove when dset leak fixed in XCE

            # get dataset counts for the batch being processed
            offset_table = self.offset_manager.getUberOffsets(uber_table.name)

            # upsert base tables where current batch delta counts > 0
            rows = []
            for row in self.offset_manager.getDatasetCounts(offset_table.name):
                if row['count'] == 0:
                    continue
                self.offset_manager.appendInfo(row['timestamp'], row['dataset'],
                                            row['count'])
                rows.append(row)
            # processes will be waiting mostly on apis responses
            num_workers = min(self.uber_universe.properties.IMDThreadCount,
                            len(rows))
            logger.debug(f'Running IMD with {num_workers} threads')
            if num_workers > 0:
                worker_args = [(row, watermark, uber_table,
                                self.uber_universe.universe_id) for row in rows]
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=num_workers) as executor:
                    results = executor.map(
                        lambda args: self.pullDeltasParallel(*args), worker_args)
                    # need to iterate for the results to raise the exception of child threads
                    for ret in results:
                        assert ret == 0

            self.offset_manager.appendTable(offset_table)
            self.reloadFormSource(watermark)
            self.offset_manager.updateBatchInfo()
        else:
            logger.debug(f'running refiner without streaming')
        logger.debug(f'__EVENT: REFINER STARTING, batch id {batch_id}')
        self.runRefiners(watermark)
        self.watermark = self.offset_manager.commitRecords(
            base_table=watermark.offsets)

        logger.debug(f'__EVENT: REFINER CYCLE FINISHED, batch id {batch_id}')
        tables = self.dispatcher.session.list_tables(pattern='ut_*')
        wobjectset = {value for (key, value) in self.watermark.mapObjects().items() if value is not None}
        table_info = {}
        for table in tables:
            if table.name in wobjectset:
                meta = table._get_meta()
                table_info[table.name] = {'size': meta.total_size_in_bytes, 'rows': meta.total_records_count}
        logger.debug(f'Table info: {table_info}')
        logger.debug(f'Watermark: {watermark.mapObjects()}')
        logger.debug(f'Low batch ids: {watermark.apps}')

        flags = get_controller_flags(self.uber_universe.universe_id,
                                     self.dispatcher.xcalarClient,
                                     self.dispatcher.session)
        while flags.paused:
            logger.info(
                f'Controller is paused, sleeping for {self.pause_freq_seconds}s'
            )
            time.sleep(self.pause_freq_seconds)
            flags = get_controller_flags(self.uber_universe.universe_id,
                                         self.dispatcher.xcalarClient,
                                         self.dispatcher.session)
        return watermark.batch_id

    def pullDeltasParallel(self, row, watermark, uber_table, universe_id):
        logging.config.dictConfig(LOGGING_CONFIG)
        structlog.configure(
            processors=[
                merge_contextvars,
                structlog.processors.KeyValueRenderer()
            ],
            context_class=structlog.threadlocal.wrap_dict(dict),
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        clear_contextvars()
        bind_contextvars(universe_id=universe_id)
        assert row['count'] > 0
        t_name = row['dataset']
        self.offset_manager.appendInfo(row['timestamp'], t_name, row['count'])
        logger.info(f'Start processing ... {t_name}')
        if t_name == 'info':
            new_delta = self.routeDelta(
                'info', self.info_schema, {}, uber_table.name,
                watermark.low_batch_id, self.info_pk_list)
            info_sandwich = self.dispatcher.executeUpsert(
                t_name, watermark.info, new_delta.name, self.info_pk_list, 1,
                'append')
            watermark.info = info_sandwich.name
        else:
            tbl = self.uber_universe.getTable(t_name, 'stream')
            if tbl is None:
                _universe = self.uber_universe.universe
                dset_list = [
                    k for k in _universe
                    if 'kafka' == _universe[k].get('stream', {
                        'type': None
                    }).get('type', 'kafka')
                ]
                logger.debug(f'***: Table: {t_name} not in universe, skipping')
                logger.debug(f'Universe: {dset_list}')
                return 0
            pklist = list(tbl.get('pks', {}).keys())
            if pklist == []:
                _universe = self.uber_universe.universe
                dset_list = [
                    k for k in _universe
                    if 'kafka' == _universe[k].get('stream', {
                        'type': None
                    }).get('type', 'kafka')
                ]
                logger.debug(f'Universe: {dset_list}')
                raise Exception(
                    f'***: Table {t_name} does not have keys defined in universe'
                )
            # Dispatch
            streaming_meta = tbl['meta']
            pk_delta_list = pklist + ['BATCH_ID']
            milestone_id = streaming_meta.get('sourceMilestoneId', 'BATCH_ID')
            new_delta = watermark.getDelta(t_name) is None
            new_delta = self.routeDelta(
                t_name,
                tbl['schema'],
                streaming_meta,
                uber_table.name,
                watermark.low_batch_id,
                pk_delta_list,
                recordField='record',
                pin_results=watermark.getDelta(t_name) is None)
            # append delta
            if watermark.getDelta(t_name):
                delta_sandwich = self.dispatcher.executeUpsert(
                    f'{t_name}_delta_staging', watermark.getDelta(t_name),
                    new_delta.name, pk_delta_list, milestone_id, '')
                watermark.setDelta(t_name, delta_sandwich.name)
                fq_name = self.dispatcher.publish_table(delta_sandwich.name)
            else:
                watermark.setDelta(t_name, new_delta.name)
                fq_name = self.dispatcher.publish_table(new_delta.name)
            logger.info(f'__PT: {fq_name}')

            # Upsert base
            milestone_id = streaming_meta.get('sourceMilestoneId', 'BATCH_ID')
            base_name = watermark.getBase(t_name)
            if not base_name:
                base_tbl = self.dispatcher.createEmptyTable(
                    tbl['schemaUpper'],
                    prefix=f'{t_name}_fromstream',
                    pks=tbl['pks'])
                base_name = base_tbl.name
            base_tbl = self.dispatcher.executeUpsert(f'{t_name}_base',
                                                     base_name, new_delta.name,
                                                     pklist, milestone_id, '')
            watermark.setBase(t_name, base_tbl.name)
            fq_name = self.dispatcher.publish_table(base_tbl.name)
            logger.info(f'__PT: {fq_name}')
        return 0

    def routeDelta(self,
                   dm,
                   schema,
                   streaming_meta,
                   uber,
                   low_batch_id,
                   pklist,
                   recordField=None,
                   pin_results=False):
        params = {'kafka': uber}
        qb = QueryBuilder(schema=schema, fatPtr=self.fatPtr)
        literals = streaming_meta.get('literals', {})
        literals = {
            l: '{}({})'.format(schema[l], literals[l]())
            for l in literals
        }
        # Flatten fat pointers & object data types
        fColumns = qb.synthesizeColumns(
            fromFP=True, recordField=recordField, upperCase=True)
        fColumns = [c for c in fColumns if not (c['destColumn'] in {**literals, 'BATCH_ID': None})] + \
            qb.synthesizeColumns(aSchema={'batch_id': 'int'}, fromFP=True, upperCase=True)
        qb.XcalarApiSynthesize(table_in='<kafka>', sameSession=False) \
            .XcalarApiFilter("eq({}::dataset, '{}')".format(self.fatPtr, dm)) \
            .XcalarApiSynthesize(columns=fColumns) \
            .XcalarApiFilter(streaming_meta.get('filter', 'eq(1,1)')) \
            .XcalarApiForceTrim(streaming_meta.get('forceTrim', False)) \
            .XcalarApiMap(literals) \
            .XcalarApiSort({k: 'PartialAscending' for k in pklist})
        dispatch_table = self.dispatcher.execute(
            qb, params=params, prefix=f'{dm}_delta', pin_results=pin_results)
        return dispatch_table

    def reloadFormSource(self, watermark):
        reload_universe = [
            k for k in self.uber_universe.universe if 'reload' == self.
            uber_universe.universe[k].get('stream', {}).get('type', None)
        ]
        if len(reload_universe):
            worker_args = []
            for t_name in reload_universe:
                worker_args.append((t_name, 'stream', watermark))
            self.loadParallel(worker_args)

    def runRefiners(self, watermark):
        logger.debug(f'__EVENT: REFINER STARTING, batch id {watermark.batch_id}')
        r_universe = self.uber_universe.getApps()
        for r in self.uber_universe.orderedApps:
            meta = r_universe[r]
            if not meta['decisionCallBack'](r, watermark):
                logger.debug(f'Not yet time to execute refiner <{r}>')
                continue
            logger.debug(f'Executing refiner <{r}> ...')
            tables = self.prepareTables(r, watermark, meta['inputs'])
            # watermark.setLowBatchId(r, watermark.batch_id + 1)
            # 2) execute
            start_dt = datetime.now().timestamp() * 1000
            runtimeParams = meta['runtimeParamsCallBack'](
                self.uber_universe.properties, self.watermark)
            logger.debug(f'Runtime Params: {runtimeParams}')
            params = {**tables, **runtimeParams}

            delta_table = self.dispatcher.executeDataflow(
                meta['dataflow'], params=params, prefix=f'{r}_app_delta')
            end_dt = datetime.now().timestamp() * 1000
            record_count = delta_table.record_count()
            self.offset_manager.appendRecord(
                start_dt, end_dt, t_name=r, record_count=record_count)
            if record_count == 0:
                logger.debug(
                    f'__EVENT: EXPORT FINISHED, NO RECORDS, batch id {watermark.batch_id}'
                )
            else:
                if 'kafka_topic' in meta.get('kafka_export_params', {}):
                    topic = meta['kafka_export_params']['kafka_topic']
                    logger.debug(
                        f'exporting {record_count} rows to {topic} topic')
                    for try_counts in range(1, 6):
                        try:
                            qb = QueryBuilder(schema=delta_table.schema) \
                                .XcalarApiSynthesize(table_in=delta_table.name, sameSession=False) \
                                .XcalarApiExport(driverName=meta['kafka_export_driver'],
                                                 driverParams=meta['kafka_export_params'])
                            self.dispatcher.execute(
                                qb,
                                schema=qb.schema,
                                desc=f'{r} Kafka export',
                                prefix=f'{r}')
                            break
                        except Exception as e:
                            logger.debug(
                                '*** WARNING exporting refiner: {}, attempt# {}'
                                .format(str(e), try_counts))
                            if try_counts == 5:
                                raise Exception(
                                    f'*** 5 unsuccessful attempts export {delta_table.name} failed, exiting'
                                )
                    logger.debug(
                        f'__EVENT: EXPORT FINISHED, RECORDS: {record_count}, batch id {watermark.batch_id}'
                    )
                if meta.get('append', False):
                    try:
                        base_table_name = watermark.getBase(r)
                    except Exception:    # fixme: check for specific error
                        base_table_name = None
                    if 'pks' in meta:
                        pk_delta_list = meta['pks']
                    else:
                        pk_delta_list = [c for c in delta_table.schema]
                    logger.debug(f'refiner: {delta_table.name}')
                    logger.debug(f'pks: {pk_delta_list}')
                    logger.debug(f'schema: {delta_table.schema}')
                    if base_table_name:
                        delta_sandwich = self.dispatcher.executeUpsert(
                            f'{r}_app_delta_staging',
                            base_table_name,
                            delta_table.name,
                            pk_delta_list,
                            milestone_id=1)
                        new_base_table_name = delta_sandwich.name
                    else:
                        new_base_table_name = delta_table.name
                    watermark.setBase(r, new_base_table_name)
                    logger.debug(
                        f'__EVENT: REFINER APPEND FINISHED, batch id {watermark.batch_id}'
                    )
                else:
                    logger.debug(
                        f'__EVENT: REFINER NOT UPSERTING, batch id {watermark.batch_id}'
                    )
                watermark.setDelta(r, delta_table.name)
            self.offset_manager.updateBatchInfo()

    def cleanLastBatch(self):
        # flags = get_controller_flags(self.uber_universe.universe_id, self.dispatcher.xcalarClient, self.dispatcher.session)
        # while flags.paused:
        #     logger.info(f'Controller is paused, sleeping for {self.pause_freq_seconds}s')
        #     time.sleep(self.pause_freq_seconds)
        #     flags = get_controller_flags(self.uber_universe.universe_id, self.dispatcher.xcalarClient, self.dispatcher.session)
        with TimerMsg(f'Purging queries') as tm:
            self.dispatcher.purge_queries()
            logger.debug(tm.get())
        with TimerMsg(f'Cleaning kvStore') as tm:
            self.dispatcher.clean_kvstore()
            logger.debug(tm.get())
        with TimerMsg(f'Deleting tables') as tm:
            tnames_to_keep = self.watermark.listObjects()
            self.dispatcher.unpinDropTables(tnames_to_keep)
            logger.debug(tm.get())

        with TimerMsg(f'Deleting dataset') as tm:
            for dsetName in list(self.watermark.getOldDatasets(
            )):    # XCE tech debt, remove when XCE is fixed
                try:
                    dataset = self.dispatcher.xcalarClient.get_dataset(
                        dsetName)
                    dataset.delete(delete_completely=True)
                    self.dispatcher.datasetCounter -= 1
                except Exception:
                    logger.warning(f'unable to delete dataset {dsetName}')
                finally:
                    self.watermark.removeOldDataset(dsetName)
            logger.debug(tm.get())

        with TimerMsg(f'Deleting retinas') as tm:
            self.dispatcher.dropRetinas()

    def checkpoint(self, key):
        statePersistor = StatePersister(
            key, self.dispatcher.xcalarClient.global_kvstore())
        old_connector = self.uber_connector
        old_universe = self.uber_universe
        old_dispatcher = self.dispatcher
        self.uber_connector = None
        self.uber_universe = None
        self.dispatcher = None
        self.offset_manager.dispatcher = None
        self.offset_manager.universe = None
        statePersistor.store_state(self)
        logger.info('orchestrator has been checkpointed.')
        self.uber_connector = old_connector
        self.uber_universe = old_universe
        self.dispatcher = old_dispatcher
        self.offset_manager.dispatcher = old_dispatcher
        self.offset_manager.universe = old_universe

    def get_snapshot_management(self, key):
        return SnapshotManagement(
            key, self.dispatcher.xcalarClient, self.dispatcher.session,
            self.dispatcher.xcalarApi, self.snapshot_data_target_name,
            self.snapshot_max_retention_number,
            self.snapshot_max_retention_period_seconds, self.uber_universe)

    def snapshot(self, key):
        orchSnapshotter = self.get_snapshot_management(key)
        old_connector = self.uber_connector
        old_universe = self.uber_universe
        old_dispatcher = self.dispatcher
        session_tables = self.watermark.mapObjects(immediatesOnly=True)
        session_tables = {
            t: session_tables[t]
            for t in session_tables if session_tables[t]
        }
        self.uber_connector = None
        self.uber_universe = None
        self.dispatcher = None
        self.offset_manager.dispatcher = None
        self.offset_manager.universe = None
        state_info = {}
        state_info['orchestrator'] = self
        orchSnapshotter.take_snapshot(session_tables, state_info)
        logger.info('orchestrator has been snapshotted.')
        self.uber_connector = old_connector
        self.uber_universe = old_universe
        self.dispatcher = old_dispatcher
        self.offset_manager.dispatcher = old_dispatcher
        self.offset_manager.universe = old_universe

    @staticmethod
    def updateState(orchestrator, client, session, xcalarApi, universe):
        if orchestrator is None:
            return None
        logger.debug(
            f'Starting State update... Session: {session.name}, Universe ID: {universe.universe_id}'
        )
        orchestrator.uber_universe = universe
        orchestrator.flat_universe = universe.getFlatUniverse(
        ) if universe is not None else None
        orchestrator.dispatcher = UberDispatcher(
            client, session=session, xcalarApi=xcalarApi)
        if orchestrator.offset_manager:
            orchestrator.offset_manager.dispatcher = orchestrator.dispatcher
            orchestrator.offset_manager.universe = orchestrator.uber_universe
        logger.debug(
            f'Updated State. Session: {session.name}, Universe ID: {universe.universe_id}'
        )
        return orchestrator

    @staticmethod
    def updateStateClient(orchestrator, client, session, xcalarApi):
        if orchestrator is None:
            return None
        logger.debug(
            f'Starting State Client update... Session: {session.name}')
        orchestrator.dispatcher = UberDispatcher(
            client, session=session, xcalarApi=xcalarApi)
        if orchestrator.offset_manager:
            orchestrator.offset_manager.dispatcher = orchestrator.dispatcher
        logger.debug(f'Updated State. Session: {session.name}')
        return orchestrator

    @staticmethod
    def upgradeObject(old, new):
        for a, value in new.__dict__.items():
            new.__dict__[a] = old.__dict__.get(a, value)
        return new

    @staticmethod
    def restoreFromCheckpoint(key, client, session, xcalarApi, universe):
        def check(t_name, s_tables):
            return t_name if t_name in s_tables else None

        statePersistor = StatePersister(key, client.global_kvstore())
        orchestrator = statePersistor.restore_state()
        if universe:
            orchestrator = Orchestrator.updateState(
                orchestrator, client, session, xcalarApi, universe)
        else:
            orchestrator = Orchestrator.updateStateClient(
                orchestrator, client, session, xcalarApi)
        if orchestrator:
            logger.info('Restoring orchestrator from checkpoint')
            w = Orchestrator.upgradeObject(orchestrator.watermark, Watermark())
            s_tables = [t.name for t in session.list_tables()]
            w.offsets = check(w.offsets, s_tables)
            w.info = check(w.info, s_tables)
            for tpl in w.objects:
                w.objects[tpl].base = check(w.objects[tpl].base, s_tables)
                w.objects[tpl].delta = check(w.objects[tpl].delta, s_tables)
            orchestrator.watermark = w
        return orchestrator

    @staticmethod
    def restoreFromSnapshot(snap, tables, client, session, xcalarApi,
                            universe):
        orchestrator = snap.state_info['orchestrator']
        orchestrator = Orchestrator.updateState(orchestrator, client, session,
                                                xcalarApi, universe)
        if orchestrator:
            logger.info('Restoring orchestrator watermark')
            orchestrator.watermark.offsets = tables['$offsets']
            orchestrator.watermark.info = tables['$info']
            for name, tid in tables.items():
                if '_delta' in name:
                    orchestrator.watermark.setDelta(name.split('_delta')[0], tid)
                else:
                    orchestrator.watermark.setBase(name, tid)
            filtered_wmos = [v for k,v in orchestrator.watermark.mapObjects(immediatesOnly=True).items() if v is not None and '/tableName/' not in v]
            if (len(filtered_wmos) != len(tables)):
                logger.debug(f'*** assertion filtered_wmos: {filtered_wmos}, tables: {tables}')
                assert(len(filtered_wmos) == len(tables))
        return orchestrator

    @staticmethod
    def restoreFromLatestSnapshot(key, client, session, xcalarApi, universe,
                                  snapshot_data_target_name,
                                  snapshot_max_retention_number,
                                  snapshot_max_retention_period_seconds):
        orchSnapshotter = SnapshotManagement(
            key, client, session, xcalarApi, snapshot_data_target_name,
            snapshot_max_retention_number,
            snapshot_max_retention_period_seconds, universe)
        snap, tables = orchSnapshotter.recover_latest_snapshot()
        return Orchestrator.restoreFromSnapshot(snap, tables, client, session,
                                                xcalarApi, universe)

    @staticmethod
    def restoreFromSpecificSnapshot(
            key, client, session, xcalarApi, universe,
            snapshot_data_target_name, snapshot_max_retention_number,
            snapshot_max_retention_period_seconds, timestamp):
        orchSnapshotter = SnapshotManagement(
            key, client, session, xcalarApi, snapshot_data_target_name,
            snapshot_max_retention_number,
            snapshot_max_retention_period_seconds, universe)
        snap, tables = orchSnapshotter.recover_specific_snapshot(timestamp)
        return Orchestrator.restoreFromSnapshot(snap, tables, client, session,
                                                xcalarApi, universe)

    def getSharedWatermark(self, universe_id):
        try:
            logger.debug(f'Getting watermark for {universe_id}')
            orchPersistor = StatePersister(
                universe_id, self.dispatcher.xcalarClient.global_kvstore())
            orchestrator = orchPersistor.restore_state()
            wos = orchestrator.watermark
            logger.debug(f'{universe_id} watermark: {wos.mapObjects()}')
            logger.debug(f'{universe_id} Low Batch ID: {wos.low_batch_id}')
            return wos
        except Exception as ex:
            raise Exception(
                f'***: Could not find Watermark for {universe_id} Universe, {str(ex)}'
            )

    def prepareTables(self, app, watermark, tables={}):
        # this_low_batch_id = watermark.getLowBatchId(app)
        pre_watermark = copy.deepcopy(watermark)
        linked_watermarks = {'self': pre_watermark}
        wos = watermark.mapObjects()
        watermark.setLowBatchId(app, watermark.batch_id + 1)
        out_t = tables.copy()
        for t_name in tables:
            tbl_name = None
            linked_universe = 'self'
            t_info = watermark.getInfo(t_name, wos)
            base_name = t_info['baseName']
            if base_name in self.uber_universe.linkedTables:
                linked_table = self.uber_universe.linkedTables[base_name]
                linked_universe = linked_table['from_universe_id']
                if linked_universe not in linked_watermarks:
                    linked_watermark = self.getSharedWatermark(linked_universe)
                    low_batch_id = linked_watermark.batch_id    # getLowBatchId(app, linked_universe)
                    watermark.setLowBatchId(app, low_batch_id + 1,
                                            linked_universe)
                    linked_watermarks[linked_universe] = linked_watermark
                else:
                    linked_watermark = linked_watermarks[linked_universe]
                tbl_name = linked_watermark.mapObjects().get(t_name, None)
                if tbl_name is None:
                    if t_info['isDelta']:
                        # delta tables could be empty in the shared universe
                        logger.debug(
                            f'***: Delta table {t_name} not active in {linked_universe} Universe, will build empty table'
                        )
                    else:
                        # base tables must be populated in the shared universe
                        raise Exception(
                            f'***: Base table {t_name} not active in {linked_universe} Universe'
                        )
                else:
                    tbl_name = f'/tableName/{linked_table["session_id"]}/{tbl_name}'
                    # update watermark with references to tables from the shared session
                    if t_info['isDelta']:
                        watermark.setDelta(base_name, tbl_name)
                    else:
                        watermark.setBase(base_name, tbl_name)
                    wos = watermark.mapObjects()
                tbl = self.uber_universe.getSchema(linked_table)
            elif not t_info['inUniverse']:
                raise Exception(
                    f'Key error: {t_name} table is not defined in the Orchestrator Universe'
                )
            else:
                tbl = self.uber_universe.getTable(base_name, 'stream')
            if not tbl:
                logger.debug(f'***: using source meta for streaming: {t_name}')    # no-kafka
                tbl = self.uber_universe.getTable(base_name, 'source')
            if tbl:
                _schema = tbl.get('schemaUpper', {})
            else:
                logger.debug(f'***: no streaming meta: {t_name}')    # refiner

            if not tbl_name:
                # if table reference not updated from a linked watermark try getting it from the main watermark
                tbl_name = wos[t_name]
            if t_info['isDelta']:
                if tbl_name is None:
                    logger.info('delta table {} is empty'.format(t_name))
                    _schema = {**_schema, 'BATCH_ID': 'int'}
                    pklist = list(tbl['pks'].keys()) + ['BATCH_ID']
                    cream_tbl = self.dispatcher.createEmptyTable(
                        _schema, prefix=f'{t_name}_empty', pks=pklist)
                    out_t[t_name] = cream_tbl.name
                    watermark.setDelta(base_name, cream_tbl.name)
                    # update watermark dictionary
                    wos = watermark.mapObjects()
                elif tbl_name.startswith(
                        f'{self.dispatcher.tgroup}_{t_name}_empty'):
                    out_t[t_name] = tbl_name
                else:
                    # TODO: resurect creamtops reuseonce the structure accounts for multiple universes
                    # creamTops = t_info['creamTops']
                    # if low_batch_id not in creamTops:
                    _low_batch_id = pre_watermark.getLowBatchId(
                        app, linked_universe)
                    _high_batch_id = linked_watermarks[
                        linked_universe].batch_id
                    qb = QueryBuilder(_schema)
                    qb.XcalarApiSynthesize(table_in=tbl_name, sameSession=False) \
                        .XcalarApiFilter(f'and(ge(BATCH_ID, {_low_batch_id}),le(BATCH_ID, {_high_batch_id}))')
                    cream_tbl = self.dispatcher.execute(
                        qb,
                        desc='realized delta',
                        prefix=f'{t_name}_delta_prep')
                    logger.debug(
                        f'Prepared {linked_universe}:{t_name} for {app}, low_batch_id: {_low_batch_id}, high_batch_id: {_high_batch_id}, this batch_id: {watermark.batch_id}'
                    )
                    out_t[t_name] = cream_tbl.name
                    # creamTops[low_batch_id] = cream_tbl.name
                    # watermark.creamTops[base_name] = creamTops
                    # else:
                    #     logger.debug(f'reusing delta_prep... {t_name}')
                    # out_t[t_name] = creamTops.get(low_batch_id)
            elif tbl_name is None:
                logger.info('base table {} is empty'.format(t_name))
                pklist = list(tbl['pks'].keys())
                base_tbl = self.dispatcher.createEmptyTable(
                    _schema, prefix=f'{t_name}_empty', pks=pklist)
                out_t[t_name] = base_tbl.name
                watermark.setBase(base_name, base_tbl.name)
            else:
                out_t[t_name] = tbl_name
        return out_t

    def getMaterializationTime(self, getMaterializationTime):
        if not hasattr(self.watermark,"materializeTime") or not getMaterializationTime:
            a = self.getAnalyzer()
            records = a.getRecords('$offsets', n=10, pre_filter='eq(batch_id, 1)')
            mt = {}
            t = list(
                map(lambda r: (r['dataset'], parse(r['start_dt']).timestamp()),
                    records))
            mt.update(t)
            self.watermark.materializeTime = mt
            return mt
        else:
            return self.watermark.materializeTime