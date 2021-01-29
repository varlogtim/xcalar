from xcalar.solutions.query_builder import QueryBuilder
from xcalar.solutions.watermark import Watermark
import copy
from datetime import datetime
from structlog import get_logger

logger = get_logger(__name__)

FATPTR = 'fatPtr'


class OffsetManager():
    schema = {
        'topic': 'string',
        'partition': 'int',
        'dataset': 'string',
        'max_offset': 'int',
        'batch_id': 'int',
        'start_dt': 'timestamp',
        'end_dt': 'timestamp',
        'count': 'int'
    }
    pks = ['start_dt', 'topic', 'partition', 'dataset', 'max_offset']

    def initialize(self, dispatcher, universe, isTimetoPurge, batch_retention_seconds,
                   params={}):    # seconds
        self.dispatcher = dispatcher
        self.universe = universe
        self.isTimetoPurge = isTimetoPurge
        self.batch_retention_seconds = batch_retention_seconds
        self.watermark = None
        self.offsets = []
        self.offset0 = None
        return self

    def startTransaction(self, watermark=None, batch_retention_seconds=None):
        self.offsets = []
        if watermark:
            self.watermark = copy.deepcopy(watermark)
            # if previous transaction did not commit successfully (e.g. it broke during refiner upsert) - remove dirty offset record
            if self.watermark.batch_id > 1:
                self.dispatcher.executePurge(
                    'offsets', self.watermark.offsets,
                    f'gt(batch_id,{self.watermark.batch_id})', self.pks, 1,
                    'rollback-dirty')
            self.watermark.batch_id += 1
            self.watermark.creamTops = {}
        else:
            self.watermark = Watermark(_objects={})
        self.start_dt = datetime.now().timestamp() * 1000
        self.watermark.purgeBatches(batch_retention_seconds)
        self.batchInfo = {}
        self.timestamp = self.start_dt
        return self.watermark

    def getBatchId(self):
        return self.watermark.batch_id

    def appendInfo(self, timestamp, t_name='-', record_count=0):
        if not self.watermark:
            raise Exception(
                'Cannot append a record without starting a transaction')
        self.batchInfo[t_name] = record_count
        self.timestamp = max(timestamp, self.timestamp)

    def appendRecord(self,
                     start_dt,
                     end_dt,
                     t_name='-',
                     record_count=0,
                     topic='-',
                     partition=-1,
                     max_offset=-1):
        if not self.watermark:
            raise Exception(
                'Cannot append a record without starting a transaction')
        self.batchInfo[t_name] = record_count
        self.timestamp = max(end_dt, self.timestamp)
        self.offsets.append({
            'dataset': t_name,
            'count': record_count,
            'start_dt': start_dt,
            'end_dt': end_dt,
            'topic': topic,
            'partition': partition,
            'max_offset': max_offset,
            'batch_id': self.watermark.batch_id
        })

    def appendTable(self, offset_table):
        off = self.dispatcher.executeUpsert('offsets', self.watermark.offsets,
                                            offset_table.name, self.pks, 1,
                                            'append')
        self.watermark.offsets = off.name

    def updateBatchInfo(self):
        # if self.batchInfo == {}:
        #     raise Exception(
        #         'Batch is empty. Should at lease have info records')
        self.batchInfo['$timestamp'] = self.timestamp
        self.watermark.batchInfo[self.watermark.batch_id] = self.batchInfo
        if self.isTimetoPurge(self.watermark):
            logger.info(f'Start to purging...')
            self.watermark.purgeBatches(self.batch_retention_seconds)
            for ds in self.watermark.objects.keys():
                tbl = self.universe.getTable(ds, 'stream')
                if tbl:
                    pklist = list(tbl.get('pks', {}).keys())
                    pk_delta_list = pklist + ['BATCH_ID']
                    base_table = self.watermark.getBase(ds)
                    if base_table:
                        if 'prune_filter' in tbl['meta']:
                            prune_filter = tbl['meta']['prune_filter']
                        else:
                            prune_filter = "eq(1,1)"
                        logger.info(f'Starting prune base_table {base_table} with filter {prune_filter}')
                        self.dispatcher.executePurge(
                            ds, base_table, prune_filter, pklist, 1, 'prune')
                    delta_table = self.watermark.getDelta(ds)
                    if delta_table:
                        min_batch_id = min(self.watermark.batchInfo.keys())
                        logger.info(f'Starting purge delta_table {delta_table} with batch_id {min_batch_id}')
                        self.dispatcher.executePurge(
                            ds, delta_table,
                            f'lt(BATCH_ID,{min_batch_id})',
                            pk_delta_list, 1, 'purge')

    def commitRecords(self, base_table=None):
        if len(self.offsets):
            qb = self.dispatcher.insertFromJson(
                schema=self.schema, records=self.offsets, execute=False)
            if base_table:
                delta = self.dispatcher.execute(
                    qb, desc='offsets commit records', prefix='offsets')
                self.dispatcher.executeUpsert(
                    'offsets', base_table, delta.name, self.pks, 1, 'append')
            else:
                qb.XcalarApiSort({k: 'PartialAscending' for k in self.pks})
                base_table = self.dispatcher.execute(
                    qb,
                    desc='offsets commit records',
                    prefix='offsets_commit',
                    pin_results=True).name
        self.watermark.offsets = base_table
        logger.debug(
            f'State: Batch contains {self.batchInfo}, batch_id: {self.watermark.batch_id}'
        )
        self.updateBatchInfo()
        w = self.watermark
        self.watermark = None
        return w

    def getUberOffsets(self, uber_name):
        # compute offsets for the current Uber batch
        qb = QueryBuilder(self.schema)
        self.watermark.uber = uber_name
        qb.XcalarApiSynthesize(table_in=uber_name, sameSession=False) \
            .XcalarApiMap({
                'topic': 'string({}::topic)'.format(FATPTR),
                'partition': 'int({}::partition)'.format(FATPTR),
                'dataset': 'string({}::dataset)'.format(FATPTR)}
        ) \
            .XcalarApiGroupBy(aggrs={
                'max_offset': 'maxInteger({}::offset)'.format(FATPTR),
                'batch_id': 'maxInteger({}::batch_id)'.format(FATPTR),
                'count': 'count(1)'
            }, group_on=['topic', 'partition', 'dataset']) \
            .XcalarApiMap({
                'start_dt': 'timestamp({})'.format(self.start_dt),
                'end_dt': 'timestamp({})'.format(datetime.now().timestamp() * 1000)}) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        return self.dispatcher.execute(
            qb, desc='Offset delta', prefix='getUberOffsets')    # .name

    def getDatasetCounts(self, offsets_name, low_batch_id=None):
        # get dataset counts within current uber batch
        schema = {
            'dataset': 'string',
            'max_batch_id': 'int',
            'timestamp': 'int',
            'count': 'int'
        }
        qb = QueryBuilder(schema)
        qb.XcalarApiSynthesize(table_in=offsets_name, sameSession=False)
        if low_batch_id:
            qb.XcalarApiFilter('ge(batch_id,{})'.format(low_batch_id))
        qb.XcalarApiMap({'timestamp': 'div(int(end_dt),1000)'}) \
            .XcalarApiGroupBy(aggrs={
                'max_batch_id': 'maxInteger(batch_id)',
                'timestamp': 'maxInteger(timestamp)',
                'count': 'sum(count)'
            }, group_on=['dataset']) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        max_offsets_tbl = self.dispatcher.execute(
            qb, desc='DatasetCounts', prefix='getDatasetCounts')
        return max_offsets_tbl.records()

    def getKafkaOffsets(self, offsets_tbl=None):
        # get latest offset and batch id per topic and partition
        if not offsets_tbl:
            offsets_tbl = self.watermark.offsets
        pks = {'topic': 'string', 'partition': 'int'}
        schema = {**pks, 'batch_id': 'int', 'max_offset': 'int'}
        qb = QueryBuilder(schema)
        qb.XcalarApiSynthesize(table_in=offsets_tbl, sameSession=False) \
            .XcalarApiFilter("neq(dataset,'info')") \
            .XcalarApiGroupBy(aggrs={
                'batch_id': 'maxInteger(batch_id)',
                'max_offset': 'maxInteger(max_offset)'
            }, group_on=['topic', 'partition']) \
            .XcalarApiSynthesize(columns=qb.synthesizeColumns())
        max_offsets_tbl = self.dispatcher.execute(
            qb, desc='getKafkaOffsets', prefix='getKafkaOffsets')
        return max_offsets_tbl.records()
