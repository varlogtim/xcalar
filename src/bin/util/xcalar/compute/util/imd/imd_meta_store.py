import time
import uuid
import json

import xcalar.compute.util.imd.imd_constants as ImdConstant
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.kvstore import KvStore
from xcalar.external.exceptions import (XDPException, IMDGroupException,
                                        NoSuchImdTableGroupException,
                                        IMDTableSchemaException)


class IMDMetaStore:
    def __init__(self, session):
        # use workbook scoped kvstore
        self.kvstore = KvStore.workbook_by_name(session.client,
                                                session.username, session.name)

    def get_group_id(self, group_name):
        try:
            key = ImdConstant.Group_name_id_map.format(
                group_name=group_name.upper())
            return self.kvstore.lookup(key)
        except XDPException as ex:
            raise NoSuchImdTableGroupException(
                "imd table group '{}' not found".format(group_name))

    def get_group_names(self, pattern='.*'):
        key = ImdConstant.Group_name_id_map.format(group_name=pattern)
        groups = self.kvstore.list(key)
        return [
            group.split(ImdConstant.Group_name_id_map.format(group_name=""))[1]
            for group in groups
        ]

    def get_group_info(self, group_name):
        group_id = self.get_group_id(group_name)
        return self._get_group_info_by_id(group_id=group_id)

    def get_table_info(self, imd_table_name):
        table_id = self._get_table_id(table_name=imd_table_name)
        return self._get_table_info(table_id)

    def get_back_xcalar_tables(self,
                               group_name,
                               table_names=None,
                               max_retries=10):
        commited_counter = None
        back_xcalar_tables = {}
        group_id = self.get_group_id(group_name=group_name)
        table_ids = {}
        retry_count = 0

        # get table_names associated to this group if not given
        if table_names is None:
            store_table_ids = self._get_group_info_by_id(
                group_id=group_id)[ImdConstant.Table_ids]
            table_names = {
                self._get_table_info(table_id=table_id)[ImdConstant.Table_name]
                for table_id in store_table_ids
            }

        table_names = set(table_names)
        for table_name in table_names:
            table_ids[self._get_table_id(table_name)] = table_name.upper()

        while retry_count < max_retries:
            commited_counter = self._get_group_info_by_id(
                group_id=group_id)[ImdConstant.Last_committed_txn_counter]
            for table_id, table_name in table_ids.items():
                table_info = self._get_table_info(table_id=table_id)
                if table_info[ImdConstant.Group_id] != group_id:
                    raise ValueError(
                        "table {} doesn't belong to the group {}".format(
                            table_name, group_name))
                if table_info[ImdConstant.
                              Last_committed_txn_counter] > commited_counter:
                    # new commit happened on the group, fetch new data
                    back_xcalar_tables.clear()
                    retry_count += 1
                    break
                back_xcalar_tables[table_name] = table_info[ImdConstant.
                                                            Back_xcalar_table]
            if len(back_xcalar_tables) == len(table_names):
                return back_xcalar_tables

        raise RuntimeError("imd table group '{}' is busy".format(group_name))

    # private methods
    def _get_group_info_by_id(self, group_id):
        key = ImdConstant.Group_info_key.format(group_id=group_id)
        return json.loads(self.kvstore.lookup(key))

    def _create_group_info(self, group_name, group_info, tables_info):
        try:
            self.get_group_info(group_name)
            raise ValueError(
                "imd table group '{}' already exists".format(group_name))
        except NoSuchImdTableGroupException:
            pass

        start = time.time()

        # prepare table group data
        group_id = str(uuid.uuid4())
        txn_id = "{}_{}".format(group_id, int(time.time()))

        self._grab_group_creation_lock(txn_id=txn_id)
        group_name = group_name.upper()

        try:
            # prepare group info keys
            group_name_id_map_key, group_name_id_map_val = ImdConstant.Group_name_id_map.format(
                group_name=group_name), group_id

            group_info[ImdConstant.
                       Group_state] = ImdConstant.IMDGroupState.NEW.name
            group_info[ImdConstant.Last_committed_txn_counter] = group_info[
                ImdConstant.Current_txn_counter] = 1
            group_info[ImdConstant.Table_ids] = []
            group_info[ImdConstant.Group_name] = group_name

            # prepare table keys
            table_id_name_tuples = []
            table_info_tuples = []
            table_schema_key_val_tuples = []

            # input schema validation and key/values preparation
            for tab in tables_info:
                table_name = tab[ImdConstant.Table_name].upper()
                # check if table exists with same name already
                self._fail_table_exists(tab[ImdConstant.Table_name])

                tab_validated = self._validate_table_schema(tab)

                table_id = "{}".format(uuid.uuid4())
                # id name map
                table_id_name_tuples.append(
                    (ImdConstant.Table_name_id_map.format(
                        group_id=group_id, table_name=table_name), table_id))
                group_info[ImdConstant.Table_ids].append(table_id)

                table_info_key = ImdConstant.Table_info_key.format(
                    table_id=table_id)
                table_info_val = json.dumps({
                    ImdConstant.Table_name: table_name,
                    ImdConstant.Group_id: group_id,
                    ImdConstant.Latest_schema_version: 1,
                    ImdConstant.Back_xcalar_table: None
                })
                table_info_tuples.append((table_info_key, table_info_val))

                tab_validated[ImdConstant.Txn_id] = txn_id
                table_schema_key_val_tuples.append(
                    (ImdConstant.Table_schema_key.format(
                        table_id=table_id, schema_version=1),
                     json.dumps(tab_validated)))

            end = time.time()

            # group details
            group_info_key, group_info_val = ImdConstant.Group_info_key.format(
                group_id=group_id), json.dumps(group_info)
            # group detail key
            group_detail_key = ImdConstant.Group_detail_key.format(
                group_id=group_id, group_counter=1)
            group_detail_value = json.dumps({
                ImdConstant.Txn_id: txn_id,
                ImdConstant.Txn_state: ImdConstant.IMDTxnState.COMMITTED.name,
                ImdConstant.Details: {
                    ImdConstant.Action: ImdConstant.IMDApi.NEW.name,
                    ImdConstant.Table_ids: group_info[ImdConstant.Table_ids]
                },
                ImdConstant.Group_counter: 1,
                ImdConstant.Start_ts: int(start),
                ImdConstant.End_ts: int(end)
            })

            kv_dict = {
                group_name_id_map_key: group_name_id_map_val,
                group_info_key: group_info_val,
                group_detail_key: group_detail_value,
            }
            for tab_key, tab_val in table_schema_key_val_tuples + table_info_tuples + table_id_name_tuples:
                kv_dict[tab_key] = tab_val
            self._update(kv_dict)
            # key to track tables marked for deletion
            self._save_deleting_tables(group_id=group_id, deleting_tables=[])
        finally:
            self._release_group_creation_lock(txn_id)

        return txn_id

    def _delete_group_info(self, group_name):
        group_name = group_name.upper()
        group_id = self.get_group_id(group_name)
        group_info = self._get_group_info_by_id(group_id=group_id)
        table_ids = group_info[ImdConstant.Table_ids]
        this_group_related_keys = set()

        # table keys
        for table_id in table_ids:
            table_name = self._get_table_info(
                table_id=table_id)[ImdConstant.Table_name]
            this_group_related_keys.update(
                self.kvstore.list(
                    ImdConstant.Table_prefix.format(table_id=table_id) + '.*'))
            this_group_related_keys.add(
                ImdConstant.Table_name_id_map.format(table_name=table_name))

        # group keys
        # transactions keys
        this_group_related_keys.update(
            self.kvstore.list(
                ImdConstant.Group_detail_key.format(
                    group_id=group_id, group_counter='.*')))
        # id name map key
        this_group_related_keys.add(
            ImdConstant.Group_name_id_map.format(group_name=group_name))
        # last group info
        this_group_related_keys.add(
            ImdConstant.Group_info_key.format(group_id=group_id))

        # this groups non persisted keys
        this_group_related_keys.update(
            self.kvstore.list(ImdConstant.Imd_non_persist_prefix +
                              ImdConstant.Group_prefix.format(
                                  group_id=group_id) + '.*'))

        # delete all keys now, already marked group info as deleted
        for key in this_group_related_keys:
            self.kvstore.delete(key)

    def _get_table_id(self, table_name):
        try:
            table_mapping_key = ImdConstant.Table_name_id_map.format(
                table_name=table_name.upper())
            return self.kvstore.lookup(table_mapping_key)
        except XDPException as ex:
            raise NoSuchImdTableGroupException(
                "No such imd table '{}' exists.".format(table_name)) from ex

    def _fail_table_exists(self, table_name):
        try:
            self._get_table_id(table_name)
            raise IMDGroupException(
                "Table {} already exists!".format(table_name))
        except NoSuchImdTableGroupException as ex:
            pass

    def _get_table_info(self, table_id):
        try:
            table_info_key = ImdConstant.Table_info_key.format(
                table_id=table_id)
            return json.loads(self.kvstore.lookup(table_info_key))
        except XDPException as ex:
            raise NoSuchImdTableGroupException(
                "No such imd table '{}'".format(table_id)) from ex

    def _grab_group_creation_lock(self, txn_id):
        key = ImdConstant.Group_creation_key
        try:
            self._internal_lock_by_key(key, txn_id)
        except Exception as ex:
            raise ValueError("Failed to grab group creation lock") from ex

    def _release_group_creation_lock(self, txn_id):
        key = ImdConstant.Group_creation_key
        try:
            self._internal_unlock_by_key(key, txn_id)
        except Exception as ex:
            raise ValueError("Failed to release group creation lock") from ex

    def _grab_txn_lock(self, group_id, txn_id):
        key = ImdConstant.Txn_lock_key.format(group_id=group_id)
        try:
            self._internal_lock_by_key(key, txn_id)
        except Exception as ex:
            raise ValueError("Failed to grab txn lock") from ex

    def _release_txn_lock(self, group_id, txn_id):
        key = ImdConstant.Txn_lock_key.format(group_id=group_id)
        try:
            self._internal_unlock_by_key(key, txn_id)
        except Exception as ex:
            raise ValueError("Failed to release txn lock") from ex

    def _grab_snapshot_lock(self, group_id, txn_id):
        key = ImdConstant.Snapshot_lock_key.format(group_id=group_id)
        try:
            self._internal_lock_by_key(key, txn_id)
        except Exception as ex:
            raise ValueError("Failed to grab snapshot lock") from ex

    def _release_snapshot_lock(self, group_id, txn_id):
        key = ImdConstant.Snapshot_lock_key.format(group_id=group_id)
        try:
            self._internal_unlock_by_key(key, txn_id)
        except Exception as ex:
            raise ValueError("Failed to release snapshot lock") from ex

    def _get_snapshots(self, group_id):
        all_snapshots_regex = ImdConstant.Snapshot_detail_key.format(
            group_id=group_id, group_counter='.*')
        all_snapshots_keys = self.kvstore.list(regex=all_snapshots_regex)
        all_snapshots = []
        if len(all_snapshots_keys) == 0:
            return all_snapshots
        for snapshot_detail_key in all_snapshots_keys:
            all_snapshots.append(
                json.loads(self.kvstore.lookup(key=snapshot_detail_key)))
        sorted_snapshots = sorted(
            all_snapshots, key=lambda x: int(x[ImdConstant.Group_counter]))
        return sorted_snapshots

    def _get_latest_snapshot(self, group_id):
        last_good_snapshot_key = ImdConstant.Snapshot_info_key.format(
            group_id=group_id)
        try:
            group_counter = json.loads(
                self.kvstore.lookup(key=last_good_snapshot_key))[
                    ImdConstant.Last_committed_txn_counter]
        except XDPException as ex:
            if ex.statusCode == StatusT.StatusKvEntryNotFound:
                return None
        snapshot_detail_key = ImdConstant.Snapshot_detail_key.format(
            group_id=group_id, group_counter=group_counter)
        return json.loads(self.kvstore.lookup(key=snapshot_detail_key))

    def _save_snapshot(self, group_id, group_counter, snapshot_info):
        kv_dict = {}
        key = ImdConstant.Snapshot_detail_key.format(
            group_id=group_id, group_counter=group_counter)
        kv_dict[key] = json.dumps(snapshot_info)
        if snapshot_info[ImdConstant.
                         Txn_state] == ImdConstant.IMDTxnState.COMMITTED.name:
            # update info to latest snapshot
            snapshot_info_key = ImdConstant.Snapshot_info_key.format(
                group_id=group_id)
            kv_dict[snapshot_info_key] = json.dumps({
                ImdConstant.Last_committed_txn_counter:
                    snapshot_info[ImdConstant.Group_counter]
            })
        self._update(kv_dict, persist=True)

    def _save_group_txn_detail(self, group_id, group_counter, txn_detail):
        kv_dict = {}
        info_key = ImdConstant.Group_info_key.format(group_id=group_id)
        info_val = self._get_group_info_by_id(group_id=group_id)
        detail_key = ImdConstant.Group_detail_key.format(
            group_id=group_id, group_counter=group_counter)
        kv_dict[detail_key] = json.dumps(txn_detail)
        if txn_detail[ImdConstant.
                      Txn_state] == ImdConstant.IMDTxnState.COMMITTED.name:
            # update info
            info_val[ImdConstant.Last_committed_txn_counter] = group_counter
            info_val[ImdConstant.
                     Group_state] = ImdConstant.IMDGroupState.IN_USE.name
            # activate the group
            activate_key = ImdConstant.Group_active_key.format(
                group_id=group_id)
            self.kvstore.add_or_replace(activate_key, json.dumps(True), False)
        elif txn_detail[ImdConstant.
                        Txn_state] == ImdConstant.IMDTxnState.BEGIN.name:
            info_val[ImdConstant.Current_txn_counter] = group_counter
        kv_dict[info_key] = json.dumps(info_val)
        self._update(kv_dict, persist=True)

    def _delete_snapshot(self, group_id, group_counter):
        snapshot_detail_key = ImdConstant.Snapshot_detail_key.format(
            group_id=group_id, group_counter=group_counter)
        self.kvstore.delete(key=snapshot_detail_key)

    def _get_txns(self, group_id):
        group_info = self._get_group_info_by_id(group_id=group_id)
        current_counter = group_info[ImdConstant.Current_txn_counter]
        txns_details = []
        for i in range(1, current_counter + 1):
            txns_details.append(
                self._get_txn_by_counter(group_id=group_id, group_counter=i))
        return txns_details

    def _get_txn_by_counter(self, group_id, group_counter):
        key = ImdConstant.Group_detail_key.format(
            group_id=group_id, group_counter=group_counter)
        result = json.loads(self.kvstore.lookup(key))
        return result

    def _get_table_delta_by_counter(self, table_id, group_counter):
        key = ImdConstant.Table_delta_key.format(
            table_id=table_id, group_counter=group_counter)
        try:
            result = json.loads(self.kvstore.lookup(key))
            return result
        except XDPException as ex:
            if ex.statusCode == StatusT.StatusKvEntryNotFound:
                return None

    def _get_df_delta_by_counter(self, table_id, group_counter):
        key = ImdConstant.Df_delta_key.format(
            table_id=table_id, group_counter=group_counter)
        try:
            result = json.loads(self.kvstore.lookup(key))
            return result
        except XDPException as ex:
            if ex.statusCode == StatusT.StatusKvEntryNotFound:
                return None

    def _activate_group(self, group_id):
        key = ImdConstant.Group_active_key.format(group_id=group_id)
        self.kvstore.add_or_replace(key, json.dumps(True), False)

    def _deactivate_group(self, group_id):
        key = ImdConstant.Group_active_key.format(group_id=group_id)
        self.kvstore.add_or_replace(key, json.dumps(False), False)

    def _check_group_active(self, group_id):
        key = ImdConstant.Group_active_key.format(group_id=group_id)
        try:
            return json.loads(self.kvstore.lookup(key))
        except XDPException as ex:
            if ex.statusCode == StatusT.StatusKvEntryNotFound:
                return False

    def _get_table_schema(self, table_id, schema_version=None):
        if schema_version is None:
            table_info = self._get_table_info(table_id)
            schema_version = table_info[ImdConstant.Latest_schema_version]
        table_schema_key = ImdConstant.Table_schema_key.format(
            table_id=table_id, schema_version=schema_version)
        return json.loads(self.kvstore.lookup(table_schema_key))

    def _save_deleting_tables(self, group_id, deleting_tables):
        group_delete_table_key = ImdConstant.Group_delete_table_key.format(
            group_id=group_id)
        self.kvstore.add_or_replace(group_delete_table_key,
                                    json.dumps(deleting_tables), False)

    def _list_deleting_table(self, group_id):
        group_delete_table_key = ImdConstant.Group_delete_table_key.format(
            group_id=group_id)
        try:
            return json.loads(self.kvstore.lookup(group_delete_table_key))
        except XDPException as ex:
            return []

    def _update(self, kv_dict, persist=True):
        self.kvstore.multi_add_or_replace(kv_dict, persist)

    # Only snapshot will call this function
    def _increase_table_ref(self, table_name):
        key = ImdConstant.Imd_table_ref_key.format(table_name=table_name)
        for _ in range(ImdConstant.Table_ref_retry):
            # retry logic
            try:
                ref = json.loads(self.kvstore.lookup(key))
                if ref == 0:
                    raise RuntimeError(
                        "Table: {} is already deleted".format(table_name))
            except XDPException as ex:
                raise RuntimeError(
                    "Failed to find the table reference count key:{}".format(
                        key))
            try:
                self.kvstore.set_if_equal(False, key, json.dumps(ref),
                                          json.dumps(ref + 1))
                break
            except XDPException as ex:
                time.sleep(1)

    # Only imd drop tables / snapshot will call this
    def _decrease_table_ref(self, table_name):
        key = ImdConstant.Imd_table_ref_key.format(table_name=table_name)
        for _ in range(ImdConstant.Table_ref_retry):
            # retry logic
            try:
                ref = json.loads(self.kvstore.lookup(key))
            except XDPException as ex:
                return
            try:
                self.kvstore.set_if_equal(False, key, json.dumps(ref),
                                          json.dumps(ref - 1))
                if ref - 1 == 0:
                    self.kvstore.delete(key)
                return
            except XDPException as ex:
                time.sleep(1)

    # Helper methods
    @staticmethod
    def _validate_table_schema(imd_table):
        if not isinstance(imd_table, dict):
            raise TypeError(
                "imd_tables have a invalid type '{}', should be all dictionaries"
                .format(type(imd_table)))

        tab_validated = {
            ImdConstant.Table_name: imd_table[ImdConstant.Table_name].upper(),
            ImdConstant.Schema: [],
            ImdConstant.PK: []
        }

        sql_data_types = ImdConstant.SqlDataType.types()

        imd_tables_info = []
        valid_imd_table_keys = {
            ImdConstant.Table_name, ImdConstant.PK, ImdConstant.Schema
        }
        if not valid_imd_table_keys.issubset(set(imd_table.keys())):
            raise IMDTableSchemaException(
                "imd_tables entries should contains these keys: {}".format(
                    ", ".join(valid_imd_table_keys)))

        # validate schema
        columns_set = set()
        for col in imd_table[ImdConstant.Schema]:
            output_col = {}
            output_col[ImdConstant.Col_type] = col[ImdConstant.
                                                   Col_type].upper()
            output_col[ImdConstant.Col_name] = col[ImdConstant.Col_name]
            if output_col[ImdConstant.Col_type] not in sql_data_types:
                raise IMDTableSchemaException(
                    "invalid col_type in schema of table '{}' and column '{}', supported types are {}"
                    .format(imd_table[ImdConstant.Table_name],
                            col[ImdConstant.Col_name],
                            ", ".join(sql_data_types)))
            columns_set.add(output_col[ImdConstant.Col_name])
            tab_validated[ImdConstant.Schema].append(output_col)

        # validate p_keys
        tab_validated[ImdConstant.PK] = [x for x in imd_table[ImdConstant.PK]]
        if not tab_validated[ImdConstant.PK]:
            raise IMDTableSchemaException(
                "should provide at least one primary_keys")
        for key in tab_validated[ImdConstant.PK]:
            if key not in columns_set:
                raise IMDTableSchemaException(
                    "key {} in primary_keys does not exists in schema for table {}"
                    .format(key, imd_table[ImdConstant.Table_name]))

        return tab_validated

    def _internal_lock_by_key(self, key, txn_id):
        value = str(txn_id)
        idle_value = ImdConstant.IMDLock.UNLOCKED.name
        try:
            self.kvstore.lookup(key)
            self.kvstore.set_if_equal(False, key, idle_value, value)
        except XDPException as ex:
            if ex.statusCode == StatusT.StatusKvEntryNotFound:
                # only add COMMITTED state
                self.kvstore.add_or_replace(key, idle_value, False)
                self._internal_lock_by_key(key, txn_id)
            else:
                raise

    def _internal_unlock_by_key(self, key, txn_id):
        value = str(txn_id)
        idle_value = ImdConstant.IMDLock.UNLOCKED.name
        self.kvstore.set_if_equal(False, key, value, idle_value)
