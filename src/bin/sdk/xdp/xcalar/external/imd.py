import json
import time
import uuid
import hashlib
import traceback
import collections
import os

from xcalar.external.exceptions import (
    XDPException, IMDTableSchemaException, ImdGroupMergeException,
    ImdGroupRestoreException, ImdGroupSnapshotException, IMDGroupException)
from xcalar.external.LegacyApi.XcalarApi import XcalarApi, XcalarApiStatusException
from xcalar.external.LegacyApi.Operators import Operators
import xcalar.external.dataflow
import xcalar.external.kvstore

from xcalar.compute.util.imd.imd_meta_store import IMDMetaStore
import xcalar.compute.util.imd.imd_constants as ImdConstant
from xcalar.compute.util.imd.imd_queries import (construct_custom_dataflow)

from xcalar.compute.localtypes.Connectors_pb2 import RemoveFileRequest
from xcalar.compute.localtypes.Workbook_pb2 import WorkbookScope


class ImdTableGroup:
    """
        Created via :meth:`create_imd_table_group <xcalar.external.session.Session.create_imd_table_group>` in an existing :class:`session <xcalar.external.session.Session>`.

    **Attributes**

    The following attributes are properties associated with the :class:`ImdTableGroup <xcalar.external.imd.ImdTableGroup>` object. They cannot be directly set.

    * **name** (:class:`str`) - Name of this ImdTableGroup
    * **backing_store_path** (:class:`str`) - Path on filesystem where snapshots and deltas are persisted

    **Methods**

    These are the list of available methods:

    * :meth:`get_last_txn_id`
    * :meth:`get_txn_logs`
    * :meth:`merge`
    * :meth:`list_tables`
    * :meth:`snapshot`
    * :meth:`restore`
    * :meth:`get_table`

    """

    def __init__(self, session, group_name):
        self._client = session.client
        self._name = group_name
        self._store = IMDMetaStore(session)
        self._id = self._store.get_group_id(self.name)
        self._session = session

        legacy_xcalar_api = XcalarApi(auth_instance=self._client.auth)
        legacy_xcalar_api.setSession(session)

        self._operator_obj = Operators(legacy_xcalar_api)
        if self.info_[ImdConstant.
                      Group_state] == ImdConstant.IMDGroupState.NEW.name:
            self.restore()

    @property
    def name(self):
        return self._name

    @property
    def info_(self):
        return self._store.get_group_info(self.name)

    @property
    def target_name(self):
        return self.info_[ImdConstant.Target_name]

    @property
    def target(self):
        return self._session.client.get_data_target(self.target_name)

    @property
    def backing_store_path(self):
        return self.info_[ImdConstant.Backing_store_path]

    def is_active(self):
        return self._store._check_group_active(group_id=self._id)

    def get_last_txn_id(self):
        """
        Returns the most recent transaction id performed on this TableGroup
        """
        last_committed_txn = self._store._get_txn_by_counter(
            group_id=self._id,
            group_counter=self.info_[ImdConstant.Last_committed_txn_counter])
        assert last_committed_txn[
            ImdConstant.Txn_state] == ImdConstant.IMDTxnState.COMMITTED.name
        return last_committed_txn[ImdConstant.Txn_id]

    def get_txn_logs(self):
        """
        Returns a list of transactions performed on this TableGroup
        """
        return self._store._get_txns(group_id=self._id)

    def rename(self, new_group_name):
        raise NotImplementedError

    def merge(self, delta_tables_info, dataflow=None, persist_deltas=True):
        """
        If dataflow is not None, execute the dataflow in the current session, which creates delta_tables. Mapping of delta tables to imd_tables are present in the delta_tables_info parameter. Otherwise, if dataflow is None, delta table in the delta_tables_info assumed to be in current session. Automatically construct a single dataflow based on the lineage of the delta tables specified.

        In either case, the delta dataflow, whether specified, or auto-generated, is persisted. Deltas are persisted if persist_delta is set to True.

        If delta dataflow starts from any data source other than from a :class:`Dataset <xcalar.external.dataset.Dataset>`,  persist_deltas must be True. Otherwise this API wil fail.
        """
        self._validate_group()

        # XXX Need to support dataflow execution produce multiple tables
        if dataflow is not None:
            raise NotImplementedError("Doesn't support dataflow updates yet")

        # Check if group is already active, if no fail
        if not self.is_active():
            raise ImdGroupMergeException("Group is inactive!")

        # Deleting the hanging unused back xcalar tables first
        self._cleanup_tables()

        # Acquire txn lock
        txn_id = "{}_{}".format(self._id, int(time.time()))
        self._store._grab_txn_lock(self._id, txn_id)
        group_info = self.info_
        current_cnt = group_info[ImdConstant.Current_txn_counter] + 1
        group_detail = {
            ImdConstant.Txn_id: txn_id,
            ImdConstant.Txn_state: ImdConstant.IMDTxnState.BEGIN.name,
            ImdConstant.Start_ts: int(time.time()),
            ImdConstant.Group_counter: current_cnt
        }

        update_step = 0    # indicate the process of update
        try:
            group_info_key = ImdConstant.Group_info_key.format(
                group_id=self._id)
            group_info[ImdConstant.Current_txn_counter] = current_cnt
            group_detail_key = ImdConstant.Group_detail_key.format(
                group_id=self._id, group_counter=current_cnt)

            kv_dict = {group_detail_key: json.dumps(group_detail)}
            self._store._update(kv_dict)
            update_step = 1

            table_delta_tuples = []
            df_delta_tuples = []
            imd_table_names = []    # this is user known imd table names
            old_base_table_names = []    # back table names for imd tables
            new_base_table_names = []
            table_ids = []
            export_outdirs = []
            delta_table_names = []
            tables_info_persisted = []
            for imd_table, delta_name in delta_tables_info.items():
                # Retrieve info of imd table
                table_id = self._store._get_table_id(imd_table)
                table_ids.append(table_id)
                table_info = self._store._get_table_info(table_id)
                tables_info_persisted.append(table_info)
                table_schema = self._store._get_table_schema(table_id)
                imd_table_names.append(table_info["table_name"])

                # Schema validation
                delta_table = self._session.get_table(delta_name)
                delta_table_names.append([delta_table.name])
                self._validate_tables_schema(table_schema[ImdConstant.Schema],
                                             delta_table)

                # Save delta dataflow to the kvstore
                delta_dataflow = delta_table.get_dataflow()
                # Check whether this dataflow starts from table. If so, must
                # persist the delta
                if self._df_starts_from_table(delta_dataflow):
                    if not persist_deltas:
                        raise ImdGroupMergeException(
                            "Delta dataflow starts from a table, have to persist delta table"
                        )
                df_delta_key = ImdConstant.Df_delta_key.format(
                    table_id=table_id, group_counter=current_cnt)
                df_delta_tuples.append((df_delta_key,
                                        delta_dataflow.optimized_query_string))

                if persist_deltas:
                    # Export delta table first
                    delta_data_path = ImdConstant.Delta_data_path.format(
                        backing_store_path=group_info[ImdConstant.
                                                      Backing_store_path],
                        txn_id=txn_id,
                        table_id=table_id)
                    # writing out deltas in csv requires correct column ordering
                    columns_to_export = [
                        col_info[ImdConstant.Col_name]
                        for col_info in table_schema[ImdConstant.Schema]
                    ]
                    columns_to_export.extend(
                        [ImdConstant.Opcode, ImdConstant.Rankover])
                    delta_table.export(
                        zip(columns_to_export, columns_to_export),
                        ImdConstant.Export_driver, {
                            "directory_path": delta_data_path,
                            "target": self.target_name,
                            "file_base": "delta"
                        })
                    data_checksum = self._calculate_checksum_of_checksums(
                        data_path=delta_data_path)
                    export_outdirs.append(delta_data_path)

                    # Table delta key update
                    table_delta_key = ImdConstant.Table_delta_key.format(
                        table_id=table_id, group_counter=current_cnt)
                    table_delta_tuples.append((table_delta_key, data_checksum))

                old_base_table_names.append(
                    table_info[ImdConstant.Back_xcalar_table])

            # do merge of all tables transactionally
            new_base_table_names = self._merge_tables(
                imd_table_names, old_base_table_names, delta_table_names)

            # Table info key update
            table_info_tuples = []
            for table_id, new_base_tab_name, table_info in zip(
                    table_ids, new_base_table_names, tables_info_persisted):
                table_info_key = ImdConstant.Table_info_key.format(
                    table_id=table_id)
                table_info[ImdConstant.Back_xcalar_table] = new_base_tab_name
                table_info[ImdConstant.
                           Last_committed_txn_counter] = current_cnt
                table_info_tuples.append((table_info_key, table_info))

            update_step = 2

            # Group info key update
            group_info[ImdConstant.Last_committed_txn_counter] = current_cnt

            # Group detail key update
            group_detail[ImdConstant.
                         Txn_state] = ImdConstant.IMDTxnState.COMMITTED.name
            group_detail[ImdConstant.End_ts] = int(time.time())
            group_detail[ImdConstant.Details] = {
                ImdConstant.Action: ImdConstant.IMDApi.MERGE.name,
                ImdConstant.Table_ids: table_ids
            }

            # XXX Call the ImdMetaStore api to update the meta info based on the
            # operation instead of doing it manually for every operations
            # Update all the metastore
            kv_dict = {
                group_detail_key: json.dumps(group_detail),
                group_info_key: json.dumps(group_info)
            }
            for tabKey, tabValue in (
                    table_delta_tuples + table_info_tuples + df_delta_tuples):
                kv_dict[tabKey] = json.dumps(tabValue)
            self._store._update(kv_dict)
            update_step = 3

            # Delete all the old back table
            self._drop_tables(old_base_table_names)
            update_step = 4

            return txn_id

        # General Error Exist
        except Exception as e:
            if update_step > 0:
                # Logging failure info
                end = time.time()
                group_detail[ImdConstant.
                             Txn_state] = ImdConstant.IMDTxnState.FAILED.name
                group_detail[ImdConstant.End_ts] = int(time.time())
                group_detail[ImdConstant.Failure_reason] = str(e)
                kv_dict = {
                    group_info_key: json.dumps(group_info),
                    group_detail_key: json.dumps(group_detail)
                }
                self._store._update(kv_dict)

                for outdir in export_outdirs:
                    self._delete_files(path=outdir)
            if update_step < 3:
                # Delete all the new back table, the merge only partially successed
                self._drop_tables(new_base_table_names)

            raise e
        finally:
            # Release txn lock
            self._store._release_txn_lock(self._id, txn_id)

    def restore(self, txn_id=None):
        """
        Restores the state of the ImdTables in a TableGroup from transaction txn_id. If txn_id is not provided, recovers to the latest committed version
        """
        self._validate_group()

        # 1) check if group is already active, if yes, nothing to restore, return
        if self.is_active():
            return

        # get all txns and filter out the failed ones
        last_valid_counter = None
        if txn_id:
            all_txns = self.get_txn_logs()
            txn_counter = None
            for txn in all_txns[::-1]:
                if txn[ImdConstant.Txn_id] == txn_id:
                    last_valid_counter = txn[ImdConstant.Current_txn_counter]
                    break
            if last_valid_counter is None:
                raise ImdGroupRestoreException(
                    "Txn with txn_id {} doesn't exists".format(txn_id))
            elif all_txns[last_valid_counter][
                    ImdConstant.
                    Txn_state] != ImdConstant.IMDTxnState.COMMITTED.name:
                raise ImdGroupRestoreException(
                    "Txn with txn_id {} is invalid to restore".format(txn_id))
        else:
            last_valid_counter = self.info_[ImdConstant.
                                            Last_committed_txn_counter]

        # get all valid txns to replay resolving restore txns
        replay_txns = self._get_all_txns_to_replay(last_valid_counter)

        if len(replay_txns) == 0:
            raise ImdGroupRestoreException("No txns to restore")

        group_txn_detail = None
        new_group = False
        new_group_counter = None
        if self.info_[ImdConstant.
                      Group_state] == ImdConstant.IMDGroupState.NEW.name:
            # no need to create a new txn for a new group
            group_txn_detail = self._store._get_txn_by_counter(
                group_id=self._id, group_counter=1)
            new_group = True
            new_txn_id = group_txn_detail[ImdConstant.Txn_id]
        else:
            new_txn_id = self._new_txn_id()

        # Grab lock and write the intent to the store
        self._store._grab_txn_lock(group_id=self._id, txn_id=new_txn_id)
        group_info = self.info_
        new_group_counter = group_info[ImdConstant.Current_txn_counter]
        if not new_group:
            new_group_counter = group_info[ImdConstant.Current_txn_counter] + 1
            group_txn_detail = {
                ImdConstant.Txn_id: new_txn_id,
                ImdConstant.Group_counter: new_group_counter,
                ImdConstant.Txn_state: ImdConstant.IMDTxnState.BEGIN.name,
                ImdConstant.Start_ts: int(time.time()),
                ImdConstant.Details: {
                    ImdConstant.Action:
                        ImdConstant.IMDApi.RESTORE.name,
                    ImdConstant.Restore_from_txn_counter:
                        replay_txns[-1][ImdConstant.Group_counter]
                }
            }

        errored = True
        back_table_mappings = {}
        new_base_table_names = []
        target_table_names = []
        delta_table_names = []
        try:
            self._store._save_group_txn_detail(
                group_id=self._id,
                group_counter=new_group_counter,
                txn_detail=group_txn_detail)

            # get valid snapshots to replay which depends on txns to replay calculated above
            replay_counter_set = {
                txn[ImdConstant.Group_counter]
                for txn in replay_txns
            }
            replay_snapshots = []
            all_snapshots = self.list_snapshots()
            ii = len(all_snapshots) - 1
            last_replay_snapshot_counter = None
            while ii >= 0:
                snapshot = all_snapshots[ii]
                if snapshot[
                        ImdConstant.
                        Txn_state] == ImdConstant.IMDTxnState.COMMITTED.name and snapshot[
                            ImdConstant.
                            Group_counter] not in replay_counter_set:
                    ii -= 1
                    continue
                last_replay_snapshot_counter = snapshot[ImdConstant.
                                                        Group_counter]
                replay_snapshots = all_snapshots[0:ii + 1]
                break

            # group snapshots by target table
            table_snapshots = {}
            for snapshot in replay_snapshots:
                if snapshot[
                        ImdConstant.
                        Txn_state] != ImdConstant.IMDTxnState.COMMITTED.name:
                    continue
                for table_detail in snapshot[ImdConstant.Details]:
                    if table_detail[ImdConstant.
                                    Table_id] not in table_snapshots:
                        if table_detail[ImdConstant.Incremental] is True:
                            raise ImdGroupRestoreException(
                                "Invalid snapshot state found for table {} and txn id {}"
                                .format(table_detail[ImdConstant.Table_id],
                                        snapshot[ImdConstant.Group_counter]))
                        table_snapshots[table_detail[ImdConstant.Table_id]] = [
                            snapshot
                        ]
                    else:
                        if table_detail[ImdConstant.Incremental] is True:
                            table_snapshots[table_detail[
                                ImdConstant.Table_id]].append(snapshot)
                        else:
                            # new full snapshot found for the table
                            table_snapshots[table_detail[
                                ImdConstant.Table_id]] = [snapshot]

            # replay the snapshots found for the tables and then delta txns

            # As tables don't have any referential constraints, it is ok to
            # replay tables in any order

            # XXX As we don't support schema evolution, reading schema of each table just once
            # This will change to read all versions of schema later
            tables_schema = {
                table_id: self._store._get_table_schema(table_id=table_id)
                for table_id in self.info_[ImdConstant.Table_ids]
            }
            for tbl_id, snpshts in table_snapshots.items():
                table_info = self._store._get_table_info(tbl_id)
                assert tbl_id not in back_table_mappings
                # replay all snapshots for this table
                for snpsht in snpshts:
                    data_path = ImdConstant.Snapshot_data_path.format(
                        backing_store_path=self.backing_store_path,
                        txn_id=snpsht[ImdConstant.Txn_id],
                        table_id=tbl_id)
                    delta_table_name = self._load_data_using_custom_dataflow(
                        data_path=data_path,
                        table_info=tables_schema[tbl_id],
                        reason="snapshot")
                    if tbl_id not in back_table_mappings:
                        # this is the base table
                        back_table_mappings[tbl_id] = {}
                        back_table_mappings[tbl_id]["name"] = table_info[
                            "table_name"]
                        back_table_mappings[tbl_id][
                            "target"] = delta_table_name
                        back_table_mappings[tbl_id]["deltas"] = []
                    else:
                        back_table_mappings[tbl_id]["deltas"].append(
                            delta_table_name)

            # replay all the batch counters after the snapshot counter
            for replay_txn in replay_txns:
                if last_replay_snapshot_counter and replay_txn[
                        ImdConstant.
                        Group_counter] <= last_replay_snapshot_counter:
                    continue
                details = replay_txn[ImdConstant.Details]
                if details[ImdConstant.
                           Action] == ImdConstant.IMDApi.ALTER.name:
                    for schema_info in details[ImdConstant.Table_ids]:
                        base_table = self._session.get_table(
                            back_table_mappings[schema_info[ImdConstant.
                                                            Table_id]])
                        new_schema = self._store._get_table_schema(
                            schema_info[ImdConstant.Table_id],
                            schema_info['after'])
                        merged_table = self._alter_table_schema(
                            back_table_mappings[schema_info[
                                ImdConstant.Table_id]], new_schema)
                        self._drop_tables([base_table.name])
                        back_table_mappings[tbl_id] = merged_table.name
                else:
                    self._replay_txn(
                        txn_info=replay_txn,
                        tables_schema=tables_schema,
                        table_mappings=back_table_mappings)

            # now we have all the snapshots and deltas loaded into memory
            # let's apply merge
            table_ids, imd_table_names, target_table_names, delta_table_names = zip(
                *[(tbl_id, contents["name"], contents["target"],
                   contents["deltas"])
                  for tbl_id, contents in back_table_mappings.items()])
            new_base_table_names = self._merge_tables(
                imd_table_names, target_table_names, delta_table_names)

            table_info_dict = {}
            for table_id, new_base_name in zip(table_ids,
                                               new_base_table_names):
                table_info_key = ImdConstant.Table_info_key.format(
                    table_id=table_id)
                table_info = self._store._get_table_info(table_id=table_id)
                table_info[ImdConstant.Back_xcalar_table] = new_base_name
                table_info[ImdConstant.
                           Last_committed_txn_counter] = new_group_counter
                table_info_dict[table_info_key] = json.dumps(table_info)
            self._store._update(table_info_dict)
            group_txn_detail[
                ImdConstant.Txn_state] = ImdConstant.IMDTxnState.COMMITTED.name
            group_txn_detail[ImdConstant.End_ts] = int(time.time())
            self._store._save_group_txn_detail(
                group_id=self._id,
                group_counter=new_group_counter,
                txn_detail=group_txn_detail)
            self._drop_tables(
                list(target_table_names) + list(delta_table_names))
            errored = False
        finally:
            # release lock
            self._store._release_txn_lock(group_id=self._id, txn_id=new_txn_id)
            if errored:
                group_txn_detail[
                    ImdConstant.
                    Txn_state] = ImdConstant.IMDTxnState.FAILED.name
                group_txn_detail[ImdConstant.End_ts] = int(time.time())
                group_txn_detail[ImdConstant.
                                 Failure_reason] = traceback.format_exc(
                                     limit=1, chain=False)
                self._store._save_group_txn_detail(
                    group_id=self._id,
                    group_counter=new_group_counter,
                    txn_detail=group_txn_detail)

                # drop tables created
                self._drop_tables(
                    list(new_base_table_names) + list(target_table_names) +
                    list(delta_table_names))

        return new_txn_id

    def _replay_txn(self, txn_info, tables_schema, table_mappings):
        '''
        Replays only new or merge transactions, the resulting backing xcalar tables
        will be updated to table_mapping dictionary
        '''
        txn_details = txn_info[ImdConstant.Details]
        if txn_details[
                ImdConstant.
                Action] != ImdConstant.IMDApi.MERGE.name and txn_details[
                    ImdConstant.Action] != ImdConstant.IMDApi.NEW.name:
            raise ImdGroupMergeException(
                "Invalid action {} found in txn {}".format(
                    txn_details[ImdConstant.Action],
                    txn_info[ImdConstant.Txn_id]))

        for tbl_id in txn_details[ImdConstant.Table_ids]:
            if txn_details[ImdConstant.
                           Action] != ImdConstant.IMDApi.MERGE.name:
                delta_table_name = self._load_data_using_custom_dataflow(
                    data_path='0',
                    table_info=tables_schema[tbl_id],
                    reason="emptyTable")
                # this is the base table
                table_info = self._store._get_table_info(tbl_id)
                assert tbl_id not in table_mappings
                table_mappings[tbl_id] = {}
                table_mappings[tbl_id]["name"] = table_info["table_name"]
                table_mappings[tbl_id]["target"] = delta_table_name
                table_mappings[tbl_id]["deltas"] = []
            else:
                # merge case
                delta_table_txn_details = self._store._get_table_delta_by_counter(
                    tbl_id, txn_info[ImdConstant.Group_counter])
                delta_df_txn_details = self._store._get_df_delta_by_counter(
                    tbl_id, txn_info[ImdConstant.Group_counter])
                data_path = ImdConstant.Delta_data_path.format(
                    backing_store_path=self.backing_store_path,
                    txn_id=txn_info[ImdConstant.Txn_id],
                    table_id=tbl_id)
                if delta_table_txn_details is not None:
                    # load the data using delta
                    delta_table_name = self._load_data_using_custom_dataflow(
                        data_path=data_path,
                        table_info=tables_schema[tbl_id],
                        reason="delta")
                elif delta_df_txn_details is not None:
                    # load the data using the dataflow
                    delta_table_name = self._load_data_using_delta_dataflow(
                        delta_df_txn_details,
                        tables_schema[tbl_id][ImdConstant.Table_name])
                else:
                    raise ImdGroupRestoreException(
                        "No delta or dataflow found for table {} and txn {} to restore"
                        .format(tbl_id, txn_info[ImdConstant.Txn_id]))

                table_mappings[tbl_id]["deltas"].append(delta_table_name)

    # this routine needs to be called to before making any IMD/restore/snapshot operation
    def _validate_group(self):
        group_info = self.info_
        if group_info[ImdConstant.
                      Group_state] == ImdConstant.IMDGroupState.DELETE.name:
            raise IMDGroupException("Group marked for deletion")

    def _df_starts_from_table(self, dataflow):
        operations = dataflow._query_list
        graph = {}
        for op in operations:
            node = op['args']['dest']
            graph[node] = {"operation": op['operation'], "parent": None}
            if 'source' in op['args']:
                parent = op['args']['source']
                graph[node]['parent'] = parent
                if parent not in graph:
                    graph[parent] = {"parent": None}
        root = [graph[key] for key in graph if graph[key]['parent'] is None]
        return any(
            'operation' not in node or node['operation'] != 'XcalarApiBulkLoad'
            for node in root)

    def deactivate(self):
        if not self.is_active():
            return
        # deactivate should check if there are any operations in progress and then deactivate and then drop all tables
        dummy_txn_id = self._new_txn_id()
        # grab locks of txn and snapshot
        try:
            self._store._grab_txn_lock(group_id=self._id, txn_id=dummy_txn_id)
            self._store._grab_snapshot_lock(
                group_id=self._id, txn_id=dummy_txn_id)
        except ValueError as ex:
            if "Failed to grab txn lock" in str(ex):
                raise IMDGroupException(
                    "cannot deactivate group '{}', transaction in progress".
                    format(self.name))
            elif "Failed to grab snapshot lock" in str(ex):
                self._store._release_txn_lock(
                    group_id=self._id, txn_id=dummy_txn_id)
                raise IMDGroupException(
                    "cannot deactivate group '{}', snapshot in progress".
                    format(self.name))

        try:
            # mark group inactive
            self._store._deactivate_group(self._id)
            # get all back tables and drop
            back_tables = []
            for table_id in self.info_[ImdConstant.Table_ids]:
                back_tables.append(
                    self._store._get_table_info(
                        table_id=table_id)[ImdConstant.Back_xcalar_table])
            self._drop_tables(back_tables)
        finally:
            self._store._release_txn_lock(
                group_id=self._id, txn_id=dummy_txn_id)
            self._store._release_snapshot_lock(
                group_id=self._id, txn_id=dummy_txn_id)

    def _get_all_txns_to_replay(self, counter):
        '''
        return all valid txns less than or equal to this counter
        '''
        txns_to_replay = collections.deque()
        while counter > 0:
            txn = self._store._get_txn_by_counter(
                group_id=self._id, group_counter=counter)
            if txn[ImdConstant.
                   Txn_state] != ImdConstant.IMDTxnState.COMMITTED.name:
                counter -= 1
                continue
            details = txn[ImdConstant.Details]
            if details[ImdConstant.Action] != ImdConstant.IMDApi.RESTORE.name:
                txns_to_replay.appendleft(txn)
                counter -= 1
            else:
                counter = details[ImdConstant.Restore_from_txn_counter]
        return txns_to_replay

    def snapshot(self, incremental=False):
        """
        Takes a snapshot of the current state of the ImdTables in this TableGroup. Will be used for recovery
        """
        if not self.is_active():
            raise ImdGroupSnapshotException("Group is inactive!")
        self._validate_group()

        if incremental is True:
            raise ValueError("Currently incremental snapshot is not supported")

        # 1. Get last committed group counter to snapshot. if New, Fail
        last_committed_counter = self.info_[ImdConstant.
                                            Last_committed_txn_counter]
        action = ImdConstant.IMDApi.RESTORE.name
        while action == ImdConstant.IMDApi.RESTORE.name:
            committed_txn = self._store._get_txn_by_counter(
                group_id=self._id, group_counter=last_committed_counter)
            assert committed_txn[
                ImdConstant.
                Txn_state] == ImdConstant.IMDTxnState.COMMITTED.name
            details = committed_txn[ImdConstant.Details]
            if details[ImdConstant.Action] == ImdConstant.IMDApi.NEW.name:
                raise ImdGroupSnapshotException("No data to snapshot")
            elif details[ImdConstant.
                         Action] != ImdConstant.IMDApi.RESTORE.name:
                break
            else:
                last_committed_counter = details[ImdConstant.
                                                 Restore_from_txn_counter]

        txn_id = committed_txn[ImdConstant.Txn_id]
        snapshot_txn_path = ImdConstant.Snapshot_txn_path.format(
            backing_store_path=self.backing_store_path, txn_id=txn_id)

        # 2. Get last successful group counter snapshotted,
        # if last committed == last snapshotted, Fail
        last_snapshot = self._store._get_latest_snapshot(group_id=self._id)
        last_snapshot_counter = 0
        if last_snapshot and last_snapshot[
                ImdConstant.Group_counter] == last_committed_counter:
            raise ImdGroupSnapshotException("No new data to snapshot")
        if last_snapshot:
            last_snapshot_counter = last_snapshot[ImdConstant.Group_counter]
        if last_committed_counter < last_snapshot_counter:
            raise ImdGroupSnapshotException(
                "Error: last snapshot counter {} is greater than last committed counter {}"
                .format(last_snapshot_counter, last_committed_counter))

        # 3. Grab lock and write the intent to the store
        snapshot_detail = {
            ImdConstant.Txn_id: txn_id,
            ImdConstant.Group_counter: last_committed_counter,
            ImdConstant.Txn_state: ImdConstant.IMDTxnState.BEGIN.name,
            ImdConstant.Start_ts: int(time.time())
        }
        self._store._grab_snapshot_lock(group_id=self._id, txn_id=txn_id)
        self._store._save_snapshot(
            group_id=self._id,
            group_counter=last_committed_counter,
            snapshot_info=snapshot_detail)

        errored = True
        try:
            # check if any state exists on disk
            try:
                self.target.list_files(
                    path=snapshot_txn_path, pattern='*', recursive=True)
                raise ImdGroupSnapshotException(
                    "Error: Untracked Snapshot data exists on disk for txn id {}"
                    .format(txn_id))
            except XDPException as ex:
                if "Directory \\'{}\\' does not exist".format(
                        snapshot_txn_path) in str(ex):
                    pass

            # 4. Find tables updated after last snapshot till latest counter
            all_table_ids = self.info_[ImdConstant.Table_ids]
            snapshot_tables = {}
            for table_id in all_table_ids:
                table_info = self._store._get_table_info(table_id)
                if ImdConstant.Last_committed_txn_counter not in table_info or table_info[
                        ImdConstant.
                        Last_committed_txn_counter] <= last_snapshot_counter:
                    # no update made on this table after snapshot
                    continue
                if ImdConstant.Back_xcalar_table not in table_info:
                    raise ImdGroupSnapshotException(
                        "No Xcalar Table found for table with id {} for txn {}"
                        .format(table_id, txn_id))
                # XXX doing full snapshot not incremental, so getting the back xcalar
                # table is enough otherwise need to get all the required deltas,
                # coalesce them to create one xcalar table
                snapshot_tables[table_id] = table_info[ImdConstant.
                                                       Back_xcalar_table]

            if len(snapshot_tables) == 0:
                raise ImdGroupSnapshotException("No data to snapshot")

            # 5. Export all the tables, for now do full snapshots
            for back_table_name in snapshot_tables.values():
                self._store._increase_table_ref(back_table_name)
            details = []
            for table_id, back_table_name in snapshot_tables.items():
                out_dir = ImdConstant.Snapshot_data_path.format(
                    backing_store_path=self.backing_store_path,
                    txn_id=txn_id,
                    table_id=table_id)
                # Increase table reference counter
                back_table = self._session.get_table(back_table_name)
                # XXX export operation will convert all the decimal/timestamp types to strings
                # so restore algorithm should convert then back to actual types
                back_table.export(
                    list(zip(back_table.columns, back_table.columns)),
                    ImdConstant.Export_driver, {
                        "target": self.target_name,
                        "directory_path": out_dir,
                        "file_base": "snapshot"
                    })
                # Decrease the table reference counter
                self._store._decrease_table_ref(back_table_name)
                table_snapshot_checksum = self._calculate_checksum_of_checksums(
                    data_path=out_dir)
                details.append({
                    ImdConstant.Table_id: table_id,
                    ImdConstant.Incremental: incremental,
                    ImdConstant.Snapshot_checksum: table_snapshot_checksum
                })

            # 6. Update the store with the results.
            snapshot_detail[ImdConstant.Details] = details
            snapshot_detail[ImdConstant.
                            Txn_state] = ImdConstant.IMDTxnState.COMMITTED.name
            snapshot_detail[ImdConstant.End_ts] = int(time.time())
            self._store._save_snapshot(
                group_id=self._id,
                group_counter=last_committed_counter,
                snapshot_info=snapshot_detail)
            errored = False
        finally:
            if errored:
                try:
                    self._delete_files(snapshot_txn_path)
                except XDPException as ex:
                    if "Directory \\'{}\\' does not exist".format(
                            snapshot_txn_path) in str(ex):
                        pass

                snapshot_detail[
                    ImdConstant.
                    Txn_state] = ImdConstant.IMDTxnState.FAILED.name
                snapshot_detail[ImdConstant.
                                Failure_reason] = traceback.format_exc(
                                    limit=1, chain=False)
                snapshot_detail[ImdConstant.End_ts] = int(time.time())
                self._store._save_snapshot(
                    group_id=self._id,
                    group_counter=last_committed_counter,
                    snapshot_info=snapshot_detail)
            self._store._release_snapshot_lock(
                group_id=self._id, txn_id=txn_id)

        return txn_id

    def list_snapshots(self):
        return self._store._get_snapshots(self._id)

    def delete_snapshots(self, txn_ids):
        if not isinstance(txn_ids, list):
            raise TypeError("txn_ids must be list, not '{}'".format(
                type(txn_ids)))

        all_snapshots = self.list_snapshots()
        txn_ids_set = set(txn_ids)
        for snapshot in all_snapshots:
            if snapshot[ImdConstant.Txn_id] not in txn_ids_set:
                continue
            txn_ids_set.remove(snapshot[ImdConstant.Txn_id])
            self._store._delete_snapshot(
                group_id=self._id,
                group_counter=snapshot[ImdConstant.Group_counter])
            snapshot_txn_path = ImdConstant.Snapshot_txn_path.format(
                backing_store_path=self.backing_store_path,
                txn_id=snapshot[ImdConstant.Txn_id])
            self._delete_files(path=snapshot_txn_path)
        if len(txn_ids_set) > 0:
            raise ValueError(
                "Snapshots associated with these txns '{}' not available to delete"
                .format(", ".join(txn_ids_set)))

    def alter_tables(self, updated_imd_tables):
        raise NotImplementedError

    def list_tables(self):
        """
        Returns list of ImdTables in this TableGroup
        """
        tables = []
        for table_id in self.info_[ImdConstant.Table_ids]:
            table_name = self._store._get_table_info(table_id)[
                ImdConstant.Back_xcalar_table]
            tables.append(self._session.get_table(table_name))
        return tables

    def get_table(self, imd_table_name):
        """
        Returns an ImdTable object with name imd_table_name
        """
        table_id = self._store._get_table_id(imd_table_name)
        table_info = self._store._get_table_info(table_id)
        back_xcalar_table = table_info[ImdConstant.Back_xcalar_table]
        return self._session.get_table(back_xcalar_table)

    def _new_txn_id(self):
        return "{}_{}".format(self._id, int(time.time()))

    def _alter_table_schema(self, base_table, new_schema):
        raise NotImplementedError("Schema evolution is not supported for now")

    def _drop_tables(self, table_names):
        # Drop the tables
        for table_name in table_names:
            self._store._decrease_table_ref(table_name)
        self._cleanup_tables(table_names=table_names)

    def _merge_tables(self, imd_table_names, target_table_names,
                      delta_tables_names):
        """
        imd_table_names -> this is the list of imd table names(user known)
        target_table_names -> list of base tables to be merged with the deltas
        delta_tables_names -> list of list of delta tables to be applied on
                            target tables in order.
        Ex:
        _merge_tables(imd_table_names=["sales", "web_sales"],
            target_table_names=["sales_base_v#1", "web_sales_v#5"],
            delta_tables_names=[["sales_delta_1", "sales_delta_2"],
                        ["web_sales_delta_1", "web_sales_delta_2"]])
        """
        assert len(imd_table_names) == len(target_table_names) == len(
            delta_tables_names)

        # XXX making a copy of the target tables before we merge deltas
        # this will change soon once backend supports transactional
        # merge operation on more than one target table
        new_table_names = []
        tmp_str = str(uuid.uuid4()).replace('-', '_')
        try:
            for imd_name, tab_name in zip(imd_table_names, target_table_names):
                dest_tab_tmp = "Xcalar_IMD_{}_{}".format(imd_name, tmp_str)
                key = ImdConstant.Imd_table_ref_key.format(
                    table_name=dest_tab_tmp)
                self._store.kvstore.add_or_replace(key, json.dumps(1), False)
                self._operator_obj.filter(tab_name, dest_tab_tmp, "eq(1, 1)")
                new_table_names.append(dest_tab_tmp)
            # call merge on all the base tables
            for base_tab_name, delta_tabs in zip(new_table_names,
                                                 delta_tables_names):
                base_tab = self._session.get_table(base_tab_name)
                # apply the deltas in the order it came
                # XXX this will change once backend supports
                # accepting list of deltas for each target table
                for delta_tab_name in delta_tabs:
                    delta_tab = self._session.get_table(delta_tab_name)
                    base_tab.merge(delta_tab)
        except XcalarApiStatusException as ex:
            # drop new_tables list
            self._operator_obj.dropTable("*_{}".format(tmp_str), True)
            raise ImdGroupMergeException("Merge operation failed") from ex
        return new_table_names

    def _calculate_checksum_of_checksums(self, data_path):
        info = {
            "schema": [{
                ImdConstant.Col_name: "checksum",
                ImdConstant.Col_type: ImdConstant.SqlDataType.STRING.name
            }]
        }
        table_name = self._load_data_using_custom_dataflow(
            data_path=data_path, table_info=info, reason="checksum")
        checksum_table = self._session.get_table(table_name)
        checksum_records = checksum_table.records()
        sorted_checksums = sorted(
            [rec["checksum"] for rec in checksum_records])
        self._drop_tables([checksum_table.name])
        return self._checksum_content(''.join(sorted_checksums))

    def _load_data_using_custom_dataflow(self, data_path, table_info, reason):
        target_name = self.target_name
        if reason == "emptyTable":
            target_name = ImdConstant.Table_generator_target
        df = construct_custom_dataflow(self._session.client, data_path,
                                       table_info, target_name, reason)
        return self._execute(df, "dataflow", table_prefix=reason)

    def _execute(self, content, op, table_prefix):
        rand_name = str(uuid.uuid4()).replace('-', '_')
        table_name = 'Xcalar_IMD_{}_{}'.format(table_prefix, rand_name)
        key = ImdConstant.Imd_table_ref_key.format(table_name=table_name)
        self._store.kvstore.add_or_replace(key, json.dumps(1), False)
        if op == "sql":
            self._execute_sql(content, table_name)
        elif op == "dataflow":
            self._execute_dataflow(content, table_name, rand_name)
        else:
            raise NotImplementedError("Invalid Operation: {}".format(op))
        return table_name

    def _execute_sql(self, sql, table_name):
        self._session.execute_sql(sql, result_table_name=table_name)

    def _execute_dataflow(self, df, table_name, query_name):
        qname_ret = self._session.execute_dataflow(
            df,
            query_name=query_name,
            table_name=table_name,
            optimized=True,
            is_async=False)

    def _load_data_using_delta_dataflow(self, optimized_query_string,
                                        table_name):
        retina_info = json.loads(json.loads(optimized_query_string)['retina'])
        query_string = retina_info['query']
        columns_info = retina_info['tables'][0]['columns']
        dataflow = xcalar.external.dataflow.Dataflow.create_dataflow_from_query_string(
            self._session.client,
            query_string=query_string,
            columns_to_export=columns_info)
        return self._execute(dataflow, "dataflow", table_prefix=table_name)

    def delete_group(self):
        self.deactivate()

        # mark for deletion
        group_info = self.info_
        group_info[ImdConstant.
                   Group_state] = ImdConstant.IMDGroupState.DELETE.name
        group_info_key = ImdConstant.Group_info_key.format(group_id=self._id)
        metastore_dict = {group_info_key: json.dumps(group_info)}
        self._store._update(metastore_dict)

        # delete on disk files
        try:
            try:
                resp = self.target.list_files(
                    path=self.backing_store_path,
                    pattern="{}*".format(self._id),
                    recursive=True)
            except XDPException as e:
                if "No files found" in str(e):
                    resp = []
            on_disk_files = resp['files']
            # filter out files
            on_disk_files = filter(lambda x: x['isDir'], on_disk_files)
            file_names = set([f['name'] for f in on_disk_files])
            for f in file_names:
                file_path = os.path.join(self.backing_store_path, f)
                try:
                    self._delete_files(path=file_path)
                except XDPException as e:
                    if 'No such file or directory' in str(e):
                        pass
                    else:
                        raise
        finally:
            # delete meta store data
            self._store._delete_group_info(group_name=self.name)

    def _delete_files(self, path):
        scope = WorkbookScope()
        scope.workbook.name.username = self._session.client.username

        req = RemoveFileRequest()
        req.path = path
        req.target_name = self.target_name
        req.scope.CopyFrom(scope)

        self._session.client._connectors_service.removeFile(req)

    def _cleanup_tables(self, table_names=[]):
        # Clean up all the tables that failed to drop
        # Run it before every merge
        hanging_table_list = self._store._list_deleting_table(self._id)
        deleting_tables = []
        for table_name in hanging_table_list + table_names:
            try:
                self._session.get_table(table_name).drop(
                    delete_completely=True)
            except Exception as e:
                if "No such table:" in str(e):
                    # The table gets deleted somewhere else
                    pass
                else:
                    deleting_tables.append(table_name)
        # For those tables which have an out-going cursor and we can't drop
        # them right now. Add to the kvstore, wait for the next update to
        # delete
        self._store._save_deleting_tables(self._id, deleting_tables)

    # ---------- helper functions --------------
    @staticmethod
    def _validate_tables_schema(base_schema, delta_table):
        imd_operation_fields = [ImdConstant.Opcode, ImdConstant.Rankover]
        if not all(op in delta_table.columns for op in imd_operation_fields):
            raise IMDTableSchemaException(
                "Delta table should have '{}' and '{}' columns".format(
                    ImdConstant.Opcode, ImdConstant.Rankover))

        delta_table_schema = delta_table.schema
        delta_schema = []
        for name, _type in delta_table_schema.items():
            if _type not in ImdConstant.Table_col_types_map:
                raise IMDTableSchemaException(
                    "Delta table has an unsupported datatype: {} for column: {}"
                    .format(_type, name))
            if name in imd_operation_fields:
                if ImdConstant.Table_col_types_map[_type] != 'INTEGER':
                    raise IMDTableSchemaException(
                        "{} in Delta table should be integer type instead of {}"
                        .format(name, _type))
                continue
            delta_schema.append({
                ImdConstant.Col_name: name,
                ImdConstant.Col_type: ImdConstant.Table_col_types_map[_type]
            })
        delta_schemas = set(
            [json.dumps(x, sort_keys=True) for x in delta_schema])
        base_schemas = set(
            [json.dumps(x, sort_keys=True) for x in base_schema])
        if delta_schemas != base_schemas:
            raise IMDTableSchemaException(
                "Schema of delta table: {} doesn't match base table :{}".
                format(delta_schema, base_schema))

    @staticmethod
    def _checksum_content(data_buf):
        return hashlib.md5(str(data_buf).encode('utf-8')).hexdigest()
