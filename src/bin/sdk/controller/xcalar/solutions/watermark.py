from datetime import datetime
from structlog import get_logger
logger = get_logger(__name__)


class Watermark:
    class Table:
        def __init__(self,
                     name,
                     base=None,
                     delta=None,
                     batch_id=None,
                     min_batch_id=None):
            self.name = name
            self.base = base
            self.delta = delta
            self.batch_id = batch_id
            self.min_batch_id = min_batch_id

    def __init__(self, _objects={}):
        self.objects = _objects
        self.uber = None
        self.offsets = None
        self.info = None
        self.batchInfo = {}
        self.batch_id = 1
        self.low_batch_id = 0
        self.apps = {}
        self.creamTops = {}
        self.curr_dataset = None
        self.old_datasets = []

    def __str__(self):
        strobjects = [f'{k}:{self.objects[k]}; ' for k in self.objects]
        return f'Batch ID: {self.batch_id} | objects: {strobjects}'

    def checkLatest(self):
        return True

    def setBase(self, alias, tid):
        # todo: validate that the object is in the universe
        obj = self.objects.get(alias, Watermark.Table(alias))
        obj.base = tid
        self.objects[alias] = obj

    def setDelta(self, alias, tid):
        obj = self.objects.get(alias, Watermark.Table(alias))
        obj.delta = tid
        self.objects[alias] = obj

    def getBase(self, alias):
        return self.objects[alias].base

    def getDelta(self, alias):
        return self.objects[alias].delta

    def setMinBatchId(self, alias, min_batch_id):
        obj = self.objects.get(alias, Watermark.Table(alias))
        obj.min_batch_id = min_batch_id
        self.objects[alias] = obj

    def getMinBatchId(self, alias):
        return self.objects[alias].min_batch_id

    # FIXME: start using set/get MinBatchId
    def setLowBatchId(self, alias, lowBatchId, universe_id='self'):
        low_batch_ids = self.apps.get(alias, {})
        low_batch_ids[universe_id] = lowBatchId
        self.apps[alias] = low_batch_ids

    def getLowBatchId(self, alias, universe_id='self'):
        obj = self.apps.get(alias, {})
        return obj.get(universe_id, 0)

    def minAppLowBatch(self, universe_id='self'):
        apps = [
            v[universe_id] for a, v in self.apps.items()
            if universe_id in v
        ]
        if len(apps) == 0:
            return 0
        else:
            return min(apps)

    def listObjects(self):
        out = [self.objects[o].base for o in self.objects] + \
              [self.objects[o].delta for o in self.objects] + [self.uber, self.offsets, self.info]
        return [o for o in out if o]

    def mapObjects(self, immediatesOnly=False):
        out = {
            **{o: self.objects[o].base
               for o in self.objects},
            **{
                '{}_delta'.format(o): self.objects[o].delta
                for o in self.objects
            }, '$info': self.info,
            '$offsets': self.offsets
        }
        if not immediatesOnly:
            out['$uber'] = self.uber
        return out

    def getInfo(self, alias, wos=None):
        wos = wos if wos else self.mapObjects()
        baseName = ''.join(alias.split('_delta'))
        out = {
            'inUniverse': alias in wos,
            'isDelta': alias.endswith('_delta'),
            'isFatPtr': alias in ['$uber'],
            'creamTops': self.creamTops.get(baseName, {}),
            'baseName': baseName,
            'id': wos.get(alias, None)
        }
        return out

    def setCurrentDataset(self, dsetName: str):
        self.curr_dataset = dsetName

    def getCurrentDataset(self):
        return self.curr_dataset

    def getOldDatasets(self):
        return self.old_datasets

    def addOldDataset(self, dsetName: str):
        self.old_datasets.append(dsetName)

    def removeOldDataset(self, dsetName: str):
        self.old_datasets.remove(dsetName)

    def purgeBatches(self, batch_retention_seconds):
        # max_extra_batches = 3
        now = datetime.now().timestamp()
        bi = self.batchInfo
        minRefinerLowBatch = self.minAppLowBatch()
        logger.debug(f'minRefinerLowBatch: {minRefinerLowBatch}')
        bs = [
            b for i, b in enumerate(bi)
            if (not batch_retention_seconds
                or now - bi[b]['$timestamp']/1000 < batch_retention_seconds)
            or b > minRefinerLowBatch or i == len(bi) - 1
        ]    # need at least one
        # if len(bs) < len(bi.keys()) - max_extra_batches:
        self.batchInfo = {b: bi[b] for b in bs}
        self.low_batch_id = min(bs) if len(bs) > 0 else 0
        logger.debug(
            f'retention period: {batch_retention_seconds}, low_batch_id: {self.low_batch_id}'
        )
