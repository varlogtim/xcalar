from xcalar.external.client import Client
from xcalar.external.session import Session
from xcalar.external.dataflow import Dataflow

import concurrent.futures
import time
from structlog import get_logger
from typing import Any, Dict
import json

from xcalar.solutions.packer import pack_to_string, unpack_from_string
from xcalar.solutions.query_builder import QueryBuilder
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.timer import Timer
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.compute.localtypes.Connectors_pb2 import RemoveFileRequest
from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope
from xcalar.solutions.workarounds.data_target_utils import DataTargetWriter, LocalFileWriter

from xcalar.solutions.logging_config import LOGGING_CONFIG
import logging
import logging.config
import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars

SNAPSHOT_KEY = 'snapshot_{}_fb230b2b-d497-4606-be1f-918d23b4ff73'
SNAPSHOT_FOLDER_FORMAT = '{}/{}'
SNAPSHOT_TABLE_FORMAT = '{}/{}/{}'

logger = get_logger(__name__)


class SnapshotManagement():
    class Snapshot():
        def __init__(self, snapshot_timestamp: int, snapshot_folder: str,
                     snapshot_table_dict: Dict[str, str],
                     snapshot_table_schema_dict: Dict[str, Dict],
                     state_info: Any):
            self.snapshot_timestamp = snapshot_timestamp
            self.snapshot_folder = snapshot_folder
            self.snapshot_table_dict = snapshot_table_dict
            self.snapshot_table_schema_dict = snapshot_table_schema_dict
            self.state_info = state_info

        def __str__(self):
            return 'Snapshot Timestamp: {} | Snapshot Folder: {} | Snapshot Table Dict: {} | Snapshot Table Schema Dict: {} | Snapshot State Info: {}'.format(
                self.snapshot_timestamp, self.snapshot_folder,
                self.snapshot_table_dict, self.snapshot_table_schema_dict,
                self.state_info)

    def __init__(self,
                 mgmt_name: str,
                 client: Client,
                 session: Session,
                 xcalarApi: XcalarApi,
                 target_name: str,
                 max_retention_number: int,
                 max_retention_period_seconds: int,
                 universe: any,
                 isList=False):
        self.mgmt_name = mgmt_name
        self.client = client
        self.kvstore = client.global_kvstore()
        self.session = session
        self.universe = universe
        self.xcalarApi = xcalarApi
        self.target_name = target_name
        self.max_retention_number = max_retention_number
        self.max_retention_period_seconds = max_retention_period_seconds
        self.snapshots = []
        self.dispatcher = UberDispatcher(self.client, self.session,
                                         self.xcalarApi)
        if not isList:
            self.dispatcher.shared_session = self.universe.properties.sharedSession
        self.__prepare()

    def take_snapshot(self, xcalar_table_ids: Dict[str, str], state_info: Any):
        # filter out published tables from other universes: '/tableName/xxx/xxx'
        logger.debug(f'__EVENT: STARTING SNAPSHOT TODISK')
        xcalar_table_ids = {
            d: v
            for d, v in xcalar_table_ids.items()
            if v.startswith(f'{self.dispatcher.tgroup}_')
        }
        logger.debug('Starting {} snapshots for {}: {}, {}'.format(
            len(xcalar_table_ids), self.mgmt_name, xcalar_table_ids,
            state_info))

        curr_time = int(time.time())

        snapshot_folder = SNAPSHOT_FOLDER_FORMAT.format(
            self.mgmt_name, curr_time)

        snapshot_table_dict = {}
        snapshot_table_schema_dict = {}
        for table_name, table_id in xcalar_table_ids.items():
            logger.debug('Table {} to be snapshotted has {} records'.format(
                table_name,
                self.dispatcher.get_table(table_id).record_count()))
            row_count = self.dispatcher.get_table(table_id).record_count()
            table_dir = SNAPSHOT_TABLE_FORMAT.format(self.mgmt_name, curr_time,
                                                     table_name)
            snapshot_table_dict[table_name] = table_dir

            schema = self.__export_snapshot(table_id, table_dir)
            snapshot_table_schema_dict[table_name] = {
                'table_id': table_id,
                'schema': schema,
                'row_count': row_count
            }

        snap = SnapshotManagement.Snapshot(
            curr_time, snapshot_folder, snapshot_table_dict,
            snapshot_table_schema_dict, state_info)
        self.snapshots.insert(0, snap)

        self.filter_snapshots(self.max_retention_number,
                              self.max_retention_period_seconds)

        kvstore_content = pack_to_string(self.snapshots)

        self.__export_snapshot_metadata(kvstore_content, snapshot_folder)

        self.kvstore.add_or_replace(
            SNAPSHOT_KEY.format(self.mgmt_name), kvstore_content, True)
        self.__delete_folders_other_than(self.snapshots)
        logger.debug(f'__EVENT: FINISHED SNAPSHOT TODISK')

    def get_snapshots(self):
        return self.snapshots

    def filter_snapshots(self, max_retention_number,
                         max_retention_period_seconds):
        curr_time = int(time.time())
        new_snapshots = [
            snapshot for snapshot in self.snapshots
            if (curr_time -
                snapshot.snapshot_timestamp) <= max_retention_period_seconds
        ]
        self.snapshots = new_snapshots[:max_retention_number]

    def recover_latest_snapshot(
            self) -> ('SnapshotManagement.Snapshot', Dataflow):
        latest = self.snapshots[0]
        return self.__recover_snapshot(latest)

    def recover_specific_snapshot(
            self, timestamp: int) -> ('SnapshotManagement.Snapshot', Dataflow):
        specific = next(
            (snapshot for snapshot in self.snapshots
             if str(snapshot.snapshot_timestamp) == str(timestamp)), None)
        if not specific:
            raise Exception(f'Could not find snapshot {timestamp}')
        return self.__recover_snapshot(specific)

    def override_snapshot_metadata(self, entry: str):
        entry = entry.encode('utf-8').decode('unicode_escape')
        self.kvstore.add_or_replace(
            SNAPSHOT_KEY.format(self.mgmt_name), entry, True)

    def clear_snapshot_metadata(self):
        self.kvstore.add_or_replace(
            SNAPSHOT_KEY.format(self.mgmt_name), '', True)

    def __recover_snapshot(
            self, snapshot) -> ('SnapshotManagement.Snapshot', Dataflow):
        snapshot_table_dict = snapshot.snapshot_table_dict
        snapshot_table_schema_dict = snapshot.snapshot_table_schema_dict
        loaded_snapshots = {}
        used_names = set()
        logger.debug(f'__EVENT: STARTING SNAPSHOT RECOVERY')

        worker_args = []
        for table_name, snapshot_loc in snapshot_table_dict.items():
            new_name = snapshot_table_schema_dict[table_name]['table_id']
            if new_name not in used_names:
                pks = self.universe.getTable(table_name)
                if pks:
                    pks = pks.get('pks', None)
                # preindex metadata tables
                elif table_name == "$info":
                    pks = ['TOPIC', 'XPU_ID', 'BATCH_ID'
                           ]    # TODO: create metaschchema in the universe
                elif table_name == "$offsets":
                    pks = [
                        'start_dt', 'topic', 'partition', 'dataset',
                        'max_offset'
                    ]    # TODO: create metaschchema in the universe
                worker_args.append(
                    (snapshot_loc,
                     snapshot_table_schema_dict[table_name]['schema'],
                     new_name, pks, self.universe.universe_id))
            loaded_snapshots[table_name] = new_name
            used_names.add(new_name)
        # num_workers = 5 if len(worker_args) > 0 else 0
        # processes will be waiting mostly on apis responses
        num_workers = min(self.universe.properties.snapshotThreadCount,
                          len(worker_args))
        logger.debug(f'Running Recovery with {num_workers} threads')
        if num_workers > 0:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=num_workers) as executor:
                results = executor.map(
                    lambda args: self.__load_snapshotfile(*args), worker_args)
                # need to iterate for the results to raise the exception of child threads
                for ret in results:
                    print(ret)
        logger.debug(f'__EVENT: FINISHED SNAPSHOT RECOVERY')

        validate_row_count = True
        for table_name in snapshot_table_schema_dict.keys():
            table_id = snapshot_table_schema_dict[table_name]['table_id']
            table = self.dispatcher.session.get_table(table_id)
            row_count = table.record_count()
            old_row_count = snapshot_table_schema_dict[table_name]['row_count']
            if row_count != old_row_count:
                validate_row_count = False
                logger.debug(
                    f'{table.name} has {row_count} rows after snapshot. \
                            It should have {old_row_count} rows')
            else:
                logger.debug(f'{table.name} : {row_count} rows')

        if not validate_row_count:
            raise Exception('Row count mismatch after snapshot recovery')

        logger.debug(
            f'{[(t.name,t._get_meta().pinned) for t in self.dispatcher.session.list_tables()]}'
        )
        return (snapshot, loaded_snapshots)

    def __export_snapshot(self, xcalar_table_id, dir):
        table = self.dispatcher.get_table(xcalar_table_id)
        qb = QueryBuilder(schema=table.schema)
        qb.XcalarApiSynthesize(table_in=xcalar_table_id, sameSession=False) \
            .XcalarApiExport(driverName='snapshot_csv',
                             driverParams={
                                 'target': self.target_name,
                                 'directory_path': dir,
                                 'file_base': 'd',
                                 'max_file_size': '500M'  # limit export file size to alleviate XCE memory issue
                             })
        self.dispatcher.execute(
            qb,
            schema=qb.schema,
            desc=f'{xcalar_table_id} snapshot',
            prefix=f'{xcalar_table_id}_snapshot')

        return table.schema

    def __export_snapshot_metadata(self, kvstore_content: str, dir: str):

        dw = DataTargetWriter(LocalFileWriter('./snapshot_backup'))
        data = {'dir': dir, 'kvstore_content': kvstore_content}
        dw.write_data('does_not_matter', 'metadata.json', json.dumps(data))
        # dw.write_data('does_not_matter', 'metadata.txt', kvstore_content)

        # dispatcher = UberDispatcher(self.client, self.session, self.xcalarApi)
        # _schema = {"snapshot_kvstorecontent": 'string'}
        # _records = [{
        #     "snapshot_kvstorecontent": kvstore_content
        # }]
        # xcalar_table = dispatcher.insertFromJson(schema=_schema, records=_records)
        # xcalar_table_id = xcalar_table.name

        # export_columns = [(row['name'], row['name'])
        #                   for row in xcalar_table.get_meta()['valueAttrs']]
        # export_driver_name = 'single_csv'
        # export_driver_params = {
        #     'target': self.target_name,
        #     'file_path': dir + '/metadata/metadata.csv'
        # }
        # export_dag_node = table_export(self.client, self.xcalarApi, xcalar_table_id, export_columns, export_driver_name, export_driver_params)
        # drop_export_nodes(self.xcalarApi, export_dag_node, True)

    def __delete_files(self, client, path, target_name):
        scope = WorkbookScope()
        scope.workbook.name.username = client.username

        req = RemoveFileRequest()
        req.path = path
        req.target_name = target_name
        req.scope.CopyFrom(scope)

        client._connectors_service.removeFile(req)

    def __delete_folders_other_than(self, snapshots):
        folder_names = [
            str(snapshot.snapshot_timestamp) for snapshot in snapshots
        ]
        target = self.client.get_data_target(self.target_name)
        with Timer('Listing files'):
            resp = target.list_files(self.mgmt_name + '/', recursive=True)
        logger.info('Current Snapshot is: {}'.format(folder_names[0]))
        for file in resp['files']:
            if (file['isDir']) and (all(
                    str(folder_name) not in file['name']
                    for folder_name in folder_names)) and (
                        '/' not in file['name']):
                logger.info('Found Old Snapshot: {}'.format(file['name']))
                with Timer('Deleting directory {}'.format(self.mgmt_name +
                                                          '/' + file['name'])):
                    try:
                        self.__delete_files(
                            self.client, self.mgmt_name + '/' + file['name'],
                            self.target_name)
                        logger.info('Deleted Old Snapshot: {}'.format(
                            file['name']))
                    except BaseException as e:
                        logger.warn(
                            f"Unable to delete {self.mgmt_name + '/' + file['name']}, Exception: {str(e)}"
                        )

    def __load_snapshotfile(self, directory, schema, name, pks, universe_id):
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
        qb = QueryBuilder(schema)
        qb.XcalarApiBulkLoadSort(
            parserArg={
                'recordDelim': '\n',
                'fieldDelim': '\t',
                'isCRLF': False,
                'linesToSkip': 1,
                'quoteDelim': '"',
                'hasHeader': True,
                'schemaFile': '',
                'schemaMode': 'loadInput',
                'dialect': 'xcalarSnapshot'
            },
            targetName=self.target_name,
            parserFnName='default:parseCsv',
            path=directory,
            pks=pks,
            fileNamePattern='*.csv.gz',
            recursive=True)
        qb.XcalarApiSynthesize(columns=qb.synthesizeColumns(fromFP=True))
        s_tbl = self.dispatcher.execute(
            qb, schema=qb.schema, forced_name=name, pin_results=True)
        fq_name = self.dispatcher.publish_table(s_tbl.name)
        logger.info(f'restored __PT: {fq_name}')

    def __prepare(self):
        try:
            stored = self.kvstore.lookup(SNAPSHOT_KEY.format(self.mgmt_name))
            self.snapshots = unpack_from_string(stored)
            if self.snapshots is not None and not isinstance(
                    self.snapshots, list):
                self.snapshots = [self.snapshots]
            if not self.snapshots:
                self.snapshots = []
        except Exception:
            logger.error(
                'There was an error looking up the snapshot entry in the kvstore. Please manually fix this using the shell.'
            )
