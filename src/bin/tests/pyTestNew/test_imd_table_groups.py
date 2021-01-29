import pytest
import tempfile
import shutil
import os
import time
import pandas as pd

from xcalar.compute.util.cluster import DevCluster
from xcalar.compute.util.Qa import XcalarQaDatasetPath

import xcalar.compute.util.imd.imd_constants as ImdConstant
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException

from xcalar.external.dataflow import Dataflow
from xcalar.external.exceptions import (XDPException, IMDTableSchemaException,
                                        NoSuchImdTableGroupException)
from xcalar.external.kvstore import KvStore
import xcalar.external.imd

pytestmark = pytest.mark.last(
    "Execute this test as late as possible since it manages its own cluster")

Opcode = ImdConstant.Opcode
Rankover = ImdConstant.Rankover
num_dataset_rows = 1000

source_udf = """
import random
import json
from faker import Faker

def gen_dataset(filepath, instream):
    inObj = json.loads(instream.read())
    if inObj["numRows"] == 0:
        return
    start = inObj["startRow"]
    end = start + inObj["numRows"]

    fake = Faker()
    fake.seed({})
    while start < end:
        yield {{"id1": start, "id2": start, "id2_1": start+1, "name": fake.name(), "float_val": start*1.0, "int_val": start}}
        start += 1
""".format(int(time.time()))

imd_tables_list = [{
    "table_name":
        "imd_tab1",
    "primary_keys": ['id1'],
    "schema": [{
        "name": "id1",
        "type": "Integer"
    }, {
        "name": "name",
        "type": "String"
    }, {
        "name": "float_val",
        "type": "Float"
    }]
},
                   {
                       "table_name":
                           "imd_tab2",
                       "primary_keys": ['id2', "id2_1"],
                       "schema": [{
                           "name": "id2",
                           "type": "Integer"
                       }, {
                           "name": "id2_1",
                           "type": "Integer"
                       }, {
                           "name": "int_val",
                           "type": "Integer"
                       }]
                   },
                   {
                       "table_name":
                           "imd_tab3",
                       "primary_keys": ["PK1", "PK2"],
                       "schema": [{
                           "name": "PK1",
                           "type": "INTEGER"
                       }, {
                           "name": "PK2",
                           "type": "INTEGER"
                       }, {
                           "name": "COL1",
                           "type": "STRING"
                       }, {
                           "name": "COL2",
                           "type": "STRING"
                       }]
                   }]

# build index keys struct for indexing
imd_tables_indexes = []
for imd_tab in imd_tables_list:
    index_keys = []
    for p_key in imd_tab["primary_keys"]:
        index_keys.append({
            "name": p_key,
            "ordering": XcalarOrderingT.XcalarOrderingPartialAscending
        })
    imd_tables_indexes.append(index_keys)

dataset_builder = None


# fixtures for the test
@pytest.fixture(scope="module", autouse=True)
def one_node_cluster():
    with DevCluster(num_nodes=1):
        yield


@pytest.fixture(scope="module")
def workbook(client):
    workbook_path = XcalarQaDatasetPath + "/dfWorkbookTests/imdDFExecutionTests.xlrdf.tar.gz"
    workbook_name = "test_imd_group"
    workbook = None
    with open(workbook_path, 'rb') as wb_file:
        wb_content = wb_file.read()
        workbook = client.upload_workbook(workbook_name, wb_content)
    yield workbook
    workbook.delete()


@pytest.fixture(scope="module")
def session(workbook):
    session = workbook.activate()
    yield session


@pytest.fixture(scope="module")
def dataset(client, workbook):
    global dataset_builder
    # Add udf
    mem_udf_name = "imd_group_udf_test"
    udf_mod = workbook.create_udf_module(mem_udf_name, source_udf)

    # Create memory target
    target_name = "qa_memory_target"
    target_type_id = "memory"
    data_target = client.add_data_target(target_name, target_type_id)

    # create dataset
    dataset_name = "testIMDServiceDs"
    path = str(num_dataset_rows)
    parser_name = "{}:gen_dataset".format(mem_udf_name)

    dataset_builder = workbook.build_dataset(
        dataset_name, data_target, path, "udf", parser_name=parser_name)
    dataset = dataset_builder.load()
    yield dataset

    dataset.delete()
    data_target.delete()
    udf_mod.delete()


@pytest.fixture(scope="module")
def merge_base(workbook, session):
    tab_idx = 2
    db = workbook.build_dataset(
        "merge_base_dataset", "Default Shared Root",
        XcalarQaDatasetPath + "/sdkIMDTest/merge_base.json", "json")
    dataset = db.load()
    project_columns = [{
        "name": "PK1",
        "type": "integer"
    }, {
        "name": "PK2",
        "type": "integer"
    }, {
        "name": "COL1",
        "type": "string"
    }, {
        "name": "COL2",
        "type": "string"
    }, {
        "name": "XCALARRANKOVER",
        "rename": Rankover,
        "type": "integer"
    }, {
        "name": "XCALAROPCODE",
        "rename": Opcode,
        "type": "integer"
    }]
    df = Dataflow.create_dataflow_from_dataset(
        session.client, dataset, project_columns=project_columns).custom_sort(
            imd_tables_indexes[tab_idx])
    session.execute_dataflow(df, table_name="merge_base", is_async=False)
    table = session.get_table("merge_base")
    yield table
    dataset.delete()


@pytest.fixture(scope="module")
def merge_delta(workbook, session):
    tab_idx = 2
    db = workbook.build_dataset(
        "merge_delta_dataset", "Default Shared Root",
        XcalarQaDatasetPath + "/sdkIMDTest/merge_delta.json", "json")
    dataset = db.load()
    project_columns = [{
        "name": "PK1",
        "type": "integer"
    }, {
        "name": "PK2",
        "type": "integer"
    }, {
        "name": "COL1",
        "type": "string"
    }, {
        "name": "COL2",
        "type": "string"
    }, {
        "name": "XCALARRANKOVER",
        "rename": Rankover,
        "type": "integer"
    }, {
        "name": "XCALAROPCODE",
        "rename": Opcode,
        "type": "integer"
    }]
    df = Dataflow.create_dataflow_from_dataset(
        session.client, dataset, project_columns=project_columns).custom_sort(
            imd_tables_indexes[tab_idx])
    session.execute_dataflow(df, table_name="merge_delta", is_async=False)
    table = session.get_table("merge_delta")
    table.show()
    yield table
    dataset.delete()


@pytest.fixture(scope="module")
def merge_expected_result(workbook, session):
    db = workbook.build_dataset(
        "merge_result_dataset", "Default Shared Root",
        XcalarQaDatasetPath + "/sdkIMDTest/merge_result.json", "json")
    dataset = db.load()
    project_columns = [{
        "name": "PK1",
        "type": "integer"
    }, {
        "name": "PK2",
        "type": "integer"
    }, {
        "name": "COL1",
        "type": "string"
    }, {
        "name": "COL2",
        "type": "string"
    }]
    df = Dataflow.create_dataflow_from_dataset(
        session.client, dataset, project_columns=project_columns)
    session.execute_dataflow(df, table_name="merge_result", is_async=False)
    table = session.get_table("merge_result")
    yield table
    dataset.delete()


def test_imd_non_persist_keys(client, workbook):
    workbook.activate()
    session_kv = KvStore.workbook_by_name(client, client.username,
                                          workbook.name)
    imd_np_prefix = ImdConstant.Imd_non_persist_prefix
    session_kv.add_or_replace(imd_np_prefix + "foo", "bar", False)
    session_kv.add_or_replace(imd_np_prefix + "foo1", "bar1", True)
    session_kv.add_or_replace("OTHER_PREFIX_foo", "bar2", False)
    session_kv.add_or_replace(imd_np_prefix + "foo1", "newBar",
                              False)    # same key not persisting now

    # test lookup
    assert session_kv.lookup(imd_np_prefix + "foo") == "bar"
    assert session_kv.lookup(imd_np_prefix + "foo1") == "newBar"
    assert session_kv.lookup("OTHER_PREFIX_foo") == "bar2"

    workbook.inactivate()
    workbook.activate()

    # do lookup again
    with pytest.raises(XDPException) as ex:
        session_kv.lookup(imd_np_prefix + "foo")
        if 'failed to lookup key' not in str(ex):
            assert False

    with pytest.raises(XDPException) as ex:
        session_kv.lookup(imd_np_prefix + "foo1")
        if 'failed to lookup key' not in str(ex):
            assert False

    assert session_kv.lookup("OTHER_PREFIX_foo") == "bar2"


def test_imd_create_with_wrong_info(session, workbook):
    group_name = "wrong_test_group"
    with tempfile.TemporaryDirectory() as back_store:
        # create group with empty primary keys
        with pytest.raises(IMDTableSchemaException) as ex:
            imd_tables = [{
                "table_name":
                    "empty_pk",
                "primary_keys": [],
                "schema": [{
                    "name": "id",
                    "type": "INTEGER"
                }, {
                    "name": "col",
                    "type": "INTEGER"
                }]
            }]
            session.create_imd_table_group(
                group_name=group_name,
                imd_tables=imd_tables,
                path_to_back_store=back_store)
        assert "should provide at least one primary_keys" in str(ex)

        # create group with primary keys not in schema
        with pytest.raises(IMDTableSchemaException) as ex:
            imd_tables = [{
                "table_name":
                    "extra_pk",
                "primary_keys": ["id1"],
                "schema": [{
                    "name": "id",
                    "type": "INTEGER"
                }, {
                    "name": "col",
                    "type": "INTEGER"
                }]
            }]
            session.create_imd_table_group(
                group_name=group_name,
                imd_tables=imd_tables,
                path_to_back_store=back_store)
        assert "key id1 in primary_keys does not exists in schema" in str(ex)

        # create group with data type not supported
        with pytest.raises(IMDTableSchemaException) as ex:
            imd_tables = [{
                "table_name":
                    "extra_pk",
                "primary_keys": ["id"],
                "schema": [{
                    "name": "id",
                    "type": "INTEGER"
                }, {
                    "name": "col",
                    "type": "DECIMAL"
                }]
            }]
            session.create_imd_table_group(
                group_name=group_name,
                imd_tables=imd_tables,
                path_to_back_store=back_store)
        assert "invalid col_type in schema" in str(ex)


def test_imd_create_get_delete(session, workbook):
    imd_tables = [imd_tables_list[0]]
    group_name = "test_group"

    with tempfile.TemporaryDirectory() as back_store:
        # Create group
        test_imd = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)

        assert test_imd.is_active()
        assert test_imd.get_table(
            imd_table_name="imd_TAB1").record_count() == 0

        validate_imd_group(session, test_imd, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)

        # test
        assert test_imd.name == group_name
        assert test_imd._id in test_imd.get_last_txn_id()
        assert len(
            test_imd._store.kvstore.list(".*IMD_group_{}.*".format(
                test_imd._id))) == 5
        table_ids = test_imd.info_[ImdConstant.Table_ids]
        for table_id in table_ids:
            assert len(
                test_imd._store.kvstore.list(
                    "IMD_table_{}.*".format(table_id))) == 2
        assert len(
            test_imd._store.kvstore.list(
                ImdConstant.Group_name_id_map.format(
                    group_name='TEST_GROUP'))) == 1
        assert len(
            test_imd._store.kvstore.list(
                ImdConstant.Table_name_id_map.format(
                    table_name='IMD_TAB1'))) == 1

        session.destroy()
        session = workbook.activate()

        # get group
        test_imd = session.get_imd_table_group(group_name=group_name)
        assert test_imd.name == group_name
        assert test_imd._id in test_imd.get_last_txn_id()

        # delete group
        test_imd.delete_group()
        assert len(
            test_imd._store.kvstore.list("IMD_group_{}.*".format(
                test_imd._id))) == 0

        with pytest.raises(NoSuchImdTableGroupException) as ex:
            session.get_imd_table_group(group_name=group_name)
            if "imd table group '{}' not found".format(group_name) not in str(
                    ex):
                assert False

        # re-create group again and delete
        test_imd = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        assert test_imd.is_active()
        validate_imd_group(session, test_imd, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)
        test_imd.delete_group()
        with pytest.raises(NoSuchImdTableGroupException) as ex:
            validate_imd_group(
                session, test_imd, group_name,
                [tab[ImdConstant.Table_name] for tab in imd_tables],
                back_store, ImdConstant.Default_target)

        # check all meta store state deleted
        assert len(
            test_imd._store.kvstore.list(".*IMD_group_{}.*".format(
                test_imd._id))) == 0
        for table_id in table_ids:
            assert len(
                test_imd._store.kvstore.list(
                    "IMD_table_{}.*".format(table_id))) == 0
        # group name and table name mapping keys
        assert len(
            test_imd._store.kvstore.list(
                ImdConstant.Group_name_id_map.format(
                    group_name=group_name.upper()))) == 0
        assert len(
            test_imd._store.kvstore.list(
                ImdConstant.Table_name_id_map.format(
                    table_name='IMD_TAB1'))) == 0


def test_table_lock(session, merge_base):
    imd_tables = [imd_tables_list[2]]
    group_name = "test_group"

    with tempfile.TemporaryDirectory() as back_store:
        # Create group
        test_imd = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        for table in test_imd.list_tables():
            with pytest.raises(XcalarApiStatusException) as e:
                table.drop()
                assert e.status == StatusT.StatusIMDTableLocked
        merge_1_txn_id = test_imd.merge({
            imd_tables[0]['table_name']: merge_base.name
        })
        for table in test_imd.list_tables():
            with pytest.raises(XcalarApiStatusException) as e:
                table.drop()
                assert e.status == StatusT.StatusIMDTableLocked

        # Delete imd group
        test_imd.delete_group()


@pytest.mark.skip(
    reason="changes in just few fields in a record is not supported yet")
def test_imd_merge(session, merge_base, merge_delta, merge_expected_result):
    imd_tables = [imd_tables_list[2]]

    group_name = "test_group"

    with tempfile.TemporaryDirectory() as back_path:
        # create group
        group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_path)
        assert group.name == group_name
        assert group._id in group.get_last_txn_id()
        table_id = group._store._get_table_id(imd_tables[0]['table_name'])

        # merge first delta (base) table
        merge_1_txn_id = group.merge({
            imd_tables[0]['table_name']: merge_base.name
        })
        # verify the kvstore meta info
        assert group.info_[ImdConstant.Current_txn_counter] == 2
        assert group._store._get_txn_by_counter(
            group_id=group._id, group_counter=2)["state"] == "COMMITTED"
        # verify the table
        merge_table1 = group.get_table(imd_tables[0]['table_name'])
        assert merge_table1.record_count() == 6

        # create delta table with mismatched schema
        delta_wrong_schema = "delta_wrong_schema"
        df1 = Dataflow.create_dataflow_from_table(session.client, merge_delta)
        df1 = df1.map(evals_list=[('absInt(0)', 'WRONGSCHEMA')])
        session.execute_dataflow(
            df1, table_name=delta_wrong_schema, is_async=False)
        with pytest.raises(IMDTableSchemaException) as e:
            merge_2_txn_id = group.merge({
                imd_tables[0]["table_name"]: delta_wrong_schema
            })
        assert "Schema of delta table" in str(e)
        # verify the txn id doesn't change
        assert merge_1_txn_id == group.get_last_txn_id()
        # verify the kvstore meta info
        assert group.info_[ImdConstant.Current_txn_counter] == 3
        assert group._store._get_txn_by_counter(
            group_id=group._id, group_counter=3)["state"] == "FAILED"

        # merge delta table
        merge_3_txn_id = group.merge({
            imd_tables[0]['table_name']: merge_delta.name
        })
        assert merge_3_txn_id == group.get_last_txn_id()
        # verify the merge results
        assert verify_table_equal(merge_expected_result,
                                  group.get_table(imd_tables[0]['table_name']))
        # verify the kvstore meta info
        assert group.info_[ImdConstant.Current_txn_counter] == 4
        assert group._store._get_txn_by_counter(
            group_id=group._id, group_counter=4)["state"] == "COMMITTED"
        merge_3_df_delta = group._store._get_df_delta_by_counter(
            table_id=table_id, group_counter=4)

        # merge the smame delta table with persist_delta equal to False
        merge_4_txn_id = group.merge({
            imd_tables[0]['table_name']: merge_delta.name
        },
                                     persist_deltas=False)
        # verify the merge results
        assert verify_table_equal(merge_expected_result,
                                  group.get_table(imd_tables[0]['table_name']))
        # verify the kvstore meta info
        assert merge_4_txn_id == group.get_last_txn_id()
        assert group.info_[ImdConstant.Current_txn_counter] == 5
        assert group._store._get_txn_by_counter(
            group_id=group._id, group_counter=5)["state"] == "COMMITTED"
        # verify the non-persistence of delta data
        merge_4_df_data = group._store._get_table_delta_by_counter(
            table_id=table_id, group_counter=5)
        assert merge_4_df_data is None
        merge_4_df_delta = group._store._get_df_delta_by_counter(
            table_id=table_id, group_counter=5)
        # verify the persisted dataflow delta are the same
        assert merge_3_df_delta == merge_4_df_delta
        # verify the persisted dataflow delta can generate the delta table
        df_table = group._load_data_using_delta_dataflow(
            merge_4_df_delta, table_name="df_table")
        assert verify_table_equal(merge_delta, df_table)

        # delete imd group
        group.delete_group()


def test_imd_group_deactive(workbook, dataset):
    tab_idx = 0
    imd_tables = [imd_tables_list[tab_idx]]
    group_name = "test_group"
    session = workbook.activate()

    with tempfile.TemporaryDirectory() as back_store:
        try:
            session.get_imd_table_group(group_name=group_name)
            assert False
        except xcalar.external.exceptions.NoSuchImdTableGroupException as ex:
            pass
        # create group
        test_imd_group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        assert test_imd_group.name == group_name
        assert test_imd_group._id in test_imd_group.get_last_txn_id()
        validate_imd_group(session, test_imd_group, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)
        tab1 = "base1"
        df1 = Dataflow.create_dataflow_from_dataset(
            session.client, dataset,
            project_columns=imd_tables[0]["schema"]).custom_sort(
                imd_tables_indexes[tab_idx])
        df1 = df1.map(evals_list=[('absInt(0)', Rankover), ('absInt(1)',
                                                            Opcode)])
        session.execute_dataflow(df1, table_name=tab1, is_async=False)
        update_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1
        })
        t1_records = list(
            test_imd_group.get_table(imd_table_name='imd_tab1').records())
        session.get_table(tab1).drop()

        assert len(test_imd_group.get_txn_logs()) == 2

        test_imd_group.deactivate()
        assert not test_imd_group.is_active()
        with pytest.raises(ValueError) as ex:
            test_imd_group.get_table(imd_table_name='imd_tab1')
            test_imd_group.get_table(imd_table_name='imd_tab2')
        assert len(test_imd_group.get_txn_logs()) == 2
        assert test_imd_group.get_last_txn_id() == update_txn_id

        restore_txn_id = test_imd_group.restore()
        assert restore_txn_id != update_txn_id
        assert len(test_imd_group.get_txn_logs()) == 3, test_imd_group.info_
        assert test_imd_group.get_last_txn_id() == restore_txn_id

        t2_records = test_imd_group.get_table(
            imd_table_name='imd_tab1').records()
        assert verify_table_records(t1_records, t2_records)

        test_imd_group.delete_group()


@pytest.mark.slow
def test_imd_snapshots(workbook, dataset):
    tab_idx = 0
    imd_tables = [imd_tables_list[tab_idx]]
    group_name = "test_group"
    session = workbook.activate()

    with tempfile.TemporaryDirectory() as back_store:
        try:
            test_imd = session.get_imd_table_group(group_name=group_name)
            assert False
        except xcalar.external.exceptions.NoSuchImdTableGroupException as ex:
            pass
        # create group
        test_imd_group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        assert test_imd_group.name == group_name
        assert test_imd_group._id in test_imd_group.get_last_txn_id()
        validate_imd_group(session, test_imd_group, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)

        try:
            test_imd_group.snapshot()
            assert False
        except xcalar.external.imd.ImdGroupSnapshotException:
            pass

        snapshots = test_imd_group.list_snapshots()
        assert len(snapshots) == 0
        validate_imd_group(session, test_imd_group, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)

        tab1 = "base1"
        df1 = Dataflow.create_dataflow_from_dataset(
            session.client, dataset,
            project_columns=imd_tables[0]["schema"]).custom_sort(
                imd_tables_indexes[tab_idx])
        df1 = df1.map(evals_list=[('absInt(0)', Rankover), ('absInt(1)',
                                                            Opcode)])
        session.execute_dataflow(df1, table_name=tab1, is_async=False)
        update_1_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1
        })

        validate_imd_group(session, test_imd_group, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)

        test_imd_group.snapshot()
        assert test_imd_group.get_table(
            "imd_TAB1").record_count() == num_dataset_rows
        validate_imd_group(session, test_imd_group, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)

        # verify the snapshot
        snapshots = test_imd_group.list_snapshots()
        assert len(snapshots) == 1
        assert snapshots[-1]['txn_id'] == update_1_txn_id
        snapshot_path = ImdConstant.Snapshot_txn_path.format(
            backing_store_path=test_imd_group.backing_store_path,
            txn_id=update_1_txn_id)
        # do list files here
        resp = test_imd_group.target.list_files(
            path=snapshot_path, pattern='*.csv.gz', recursive=True)
        data_files = resp['files']
        assert len(data_files) > 0
        resp = test_imd_group.target.list_files(
            path=snapshot_path, pattern='*.checksum', recursive=True)
        checksum_files = resp['files']
        assert len(checksum_files) > 0 and len(checksum_files) == len(
            data_files)
        resp = test_imd_group.target.list_files(
            path=snapshot_path, pattern='*', recursive=True)
        all_files = resp['files']
        assert len(all_files) == len(checksum_files) + len(data_files) + 1

        # alter schema --> not supported yet
        # snapshots.alter_table()

        # do snapshot and expect failure
        try:
            test_imd_group.snapshot()
            assert False
        except xcalar.external.imd.ImdGroupSnapshotException:
            pass

        # do another update
        # create delta_table1 and update imd_tab1 by add float_val by 1
        df3 = Dataflow.create_dataflow_from_table(
            session.client, session.get_table(tab1)).map(
                evals_list=[('add(float_val, 1)',
                             'float_val'), ('absInt(1)',
                                            Rankover), ('absInt(3)', Opcode)])
        tab1_delta = "delta1_tab1"
        session.execute_dataflow(df3, table_name=tab1_delta, is_async=False)
        txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1_delta
        })
        assert txn_id == test_imd_group.get_last_txn_id()
        update_2_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1
        })

        # snapshot again
        test_imd_group.snapshot()

        # verify the snapshot again
        snapshots = test_imd_group.list_snapshots()
        assert len(snapshots) == 2
        assert snapshots[-1]['txn_id'] == update_2_txn_id
        validate_imd_group(session, test_imd_group, group_name,
                           [tab[ImdConstant.Table_name] for tab in imd_tables],
                           back_store, ImdConstant.Default_target)

        # delete the old snapshot
        test_imd_group.delete_snapshots([update_1_txn_id])
        delete_snapshot_path = ImdConstant.Snapshot_txn_path.format(
            backing_store_path=test_imd_group.backing_store_path,
            txn_id=update_1_txn_id)

        # verify deletion
        # verify the snapshot again
        snapshots = test_imd_group.list_snapshots()
        assert len(snapshots) == 1
        assert snapshots[-1]['txn_id'] == update_2_txn_id
        with pytest.raises(XDPException) as ex:
            test_imd_group.target.list_files(
                path=delete_snapshot_path, pattern='*', recursive=True)

        # do snapshot and expect failure
        try:
            test_imd_group.snapshot()
            assert False
        except xcalar.external.imd.ImdGroupSnapshotException:
            pass

        # delete all snapshots
        test_imd_group.delete_snapshots([update_2_txn_id])

        # verify nothing exists
        snapshots = test_imd_group.list_snapshots()
        assert len(snapshots) == 0
        delete_snapshot_path = os.path.join(test_imd_group.backing_store_path,
                                            'snapshot')
        resp = test_imd_group.target.list_files(
            path=delete_snapshot_path, pattern='*', recursive=True)
        assert len(resp['files']) == 0

        # delete a snapshot of a valid txn_id and expect to fail
        try:
            test_imd_group.delete_snapshots([update_2_txn_id])
            assert False
        except ValueError:
            pass

        test_imd_group.delete_group()

        files = os.listdir(back_store)
        assert len(files) == 2    # should just contain deltas and snapshot dir
        delta_files = os.listdir(os.path.join(back_store, "delta"))
        assert len(delta_files) == 0
        snapshot_files = os.listdir(os.path.join(back_store, "snapshot"))
        assert len(snapshot_files) == 0


@pytest.mark.slow
@pytest.mark.parametrize("persist_delta_flag", [True, False])
def test_imd_restore_from_deltas_only(workbook, dataset, persist_delta_flag):
    tab_idx = 0
    imd_tables = [imd_tables_list[tab_idx]]

    group_name = "test_group"
    workbook.inactivate()
    session = workbook.activate()
    # XXX doing this to workaround the bug https://xcalar.atlassian.net/browse/SDK-759
    # remove this try/except in all places in this file once it is fixed
    try:
        dataset_builder.load()
    except Exception as ex:
        pass

    with tempfile.TemporaryDirectory() as back_store:
        # create group
        test_imd_group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        assert test_imd_group.name == group_name
        assert test_imd_group._id in test_imd_group.get_last_txn_id()

        # add base
        tab1 = "base1"
        dataset_df = Dataflow.create_dataflow_from_dataset(
            session.client, dataset,
            project_columns=imd_tables[0]["schema"]).custom_sort(
                imd_tables_indexes[tab_idx])
        df1 = dataset_df.map(evals_list=[('absInt(0)',
                                          Rankover), ('absInt(1)', Opcode)])
        session.execute_dataflow(df1, table_name=tab1, is_async=False)

        update_1_txn_id = test_imd_group.merge(
            {
                imd_tables[0]["table_name"]: tab1
            },
            persist_deltas=persist_delta_flag)
        assert update_1_txn_id == test_imd_group.get_last_txn_id()

        # add delta
        df2 = dataset_df.map(
            evals_list=[('add(float_val, 1)',
                         'float_val'), ('absInt(1)',
                                        Rankover), ('absInt(1)', Opcode)])
        tab1_delta = "delta1_tab1"
        session.execute_dataflow(df2, table_name=tab1_delta, is_async=False)
        update_2_txn_id = test_imd_group.merge(
            {
                imd_tables[0]["table_name"]: tab1_delta
            },
            persist_deltas=persist_delta_flag)
        assert update_2_txn_id == test_imd_group.get_last_txn_id()

        # convert source to pandas before you deactivate
        update_2_pd = pd.DataFrame.from_dict(
            test_imd_group.get_table(imd_tables[0]["table_name"]).records())
        _check_restore_with_source(workbook, test_imd_group, [update_2_pd],
                                   [imd_tables[0]["table_name"]], ['id1'])

        files = os.listdir(back_store)
        if persist_delta_flag:
            assert len(files) == 1    # should just contain deltas dir
            delta_files = os.listdir(os.path.join(back_store, "delta"))
            assert len(delta_files) == 2    # two transactions
        else:
            assert len(files) == 0

        test_imd_group.delete_group()

        # check on-disk files deleted?
        files = os.listdir(back_store)
        if persist_delta_flag:
            assert len(files) == 1    # should just contain deltas dir
            delta_files = os.listdir(os.path.join(back_store, "delta"))
            assert len(delta_files) == 0
        else:
            assert len(files) == 0


@pytest.mark.slow
def test_imd_restore_from_snapshot(workbook, dataset):
    tab_idx = 0
    imd_tables = [imd_tables_list[tab_idx]]

    group_name = "test_group"
    workbook.inactivate()
    session = workbook.activate()
    try:
        dataset_builder.load()
    except Exception as ex:
        pass

    with tempfile.TemporaryDirectory() as back_store:
        # create group
        test_imd_group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        assert test_imd_group.name == group_name
        assert test_imd_group._id in test_imd_group.get_last_txn_id()

        # restore without any update, no op
        test_imd_group.restore()

        # add base
        tab1 = "base1"
        dataset_df = Dataflow.create_dataflow_from_dataset(
            session.client, dataset,
            project_columns=imd_tables[0]["schema"]).custom_sort(
                imd_tables_indexes[tab_idx])
        df1 = dataset_df.map(evals_list=[('absInt(0)',
                                          Rankover), ('absInt(1)', Opcode)])
        session.execute_dataflow(df1, table_name=tab1, is_async=False)

        update_1_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1
        })
        assert update_1_txn_id == test_imd_group.get_last_txn_id()

        # do snapshot
        test_imd_group.snapshot()

        # add delta
        df2 = dataset_df.map(
            evals_list=[('add(float_val, 1)',
                         'float_val'), ('absInt(1)',
                                        Rankover), ('absInt(1)', Opcode)])
        tab1_delta = "delta1_tab1"
        session.execute_dataflow(df2, table_name=tab1_delta, is_async=False)
        update_2_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1_delta
        })
        assert update_2_txn_id == test_imd_group.get_last_txn_id()

        # add another delta and do snapshot to see if data exists
        df3 = dataset_df.map(evals_list=[(
            'add(float_val, 1)',
            'float_val'), ('absInt(1)',
                           Rankover), ('absInt(1)',
                                       Opcode)]).filter('gt(id1, 500)')
        tab1_delta = "delta1_tab2"
        session.execute_dataflow(df3, table_name=tab1_delta, is_async=False)
        update_2_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1_delta
        })
        assert update_2_txn_id == test_imd_group.get_last_txn_id()

        # do snapshot
        test_imd_group.snapshot()

        df3 = dataset_df.map(
            evals_list=[('add(float_val, 1)',
                         'float_val'), ('absInt(1)',
                                        Rankover), ('absInt(0)', Opcode)])
        tab1_delta = "delta1_tab3"
        session.execute_dataflow(df3, table_name=tab1_delta, is_async=False)
        update_2_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1_delta
        })
        assert update_2_txn_id == test_imd_group.get_last_txn_id()

        # do snapshot
        test_imd_group.snapshot()

        # convert source to pandas before you deactivate
        source_pd = pd.DataFrame.from_dict(
            test_imd_group.get_table(imd_tables[0]["table_name"]).records())
        _check_restore_with_source(workbook, test_imd_group, [source_pd],
                                   [imd_tables[0]["table_name"]], ['id1'])

        test_imd_group.delete_group()

        files = os.listdir(back_store)
        assert len(files) == 2    # should just contain deltas and snapshot dir
        delta_files = os.listdir(os.path.join(back_store, "delta"))
        assert len(delta_files) == 0
        snapshot_files = os.listdir(os.path.join(back_store, "snapshot"))
        assert len(snapshot_files) == 0


def test_imd_restore_corrupted_checksum(workbook, dataset):
    tab_idx = 0
    imd_tables = [imd_tables_list[tab_idx]]

    group_name = "test_group"
    workbook.inactivate()
    session = workbook.activate()
    try:
        dataset_builder.load()
    except Exception as ex:
        pass

    with tempfile.TemporaryDirectory() as back_store:
        # create group
        test_imd_group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        assert test_imd_group.name == group_name
        assert test_imd_group._id in test_imd_group.get_last_txn_id()

        # add base
        tab1 = "base1"
        dataset_df = Dataflow.create_dataflow_from_dataset(
            session.client, dataset,
            project_columns=imd_tables[0]["schema"]).custom_sort(
                imd_tables_indexes[tab_idx])

        df1 = dataset_df.map(evals_list=[('absInt(0)',
                                          Rankover), ('absInt(1)', Opcode)])
        session.execute_dataflow(df1, table_name=tab1, is_async=False)

        update_1_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1
        })
        assert update_1_txn_id == test_imd_group.get_last_txn_id()

        # corrupt the checksum file
        source_pd = pd.DataFrame.from_dict(
            test_imd_group.get_table(imd_tables[0]["table_name"]).records())
        table_id = test_imd_group.info_[ImdConstant.Table_ids][0]
        delta_path = ImdConstant.Delta_data_path.format(
            backing_store_path=back_store,
            txn_id=update_1_txn_id,
            table_id=table_id)
        resp = test_imd_group.target.list_files(
            path=delta_path, pattern='*.checksum', recursive=False)
        checksum_files = resp['files']
        assert len(checksum_files) > 0
        checksum_path = os.path.join(delta_path, checksum_files[0]['name'])
        with open(checksum_path, 'w+') as fp:
            fp.write("some junk")
        with pytest.raises(XcalarApiStatusException):
            _check_restore_with_source(workbook, test_imd_group, [source_pd],
                                       [imd_tables[0]["table_name"]], ['id1'])

        test_imd_group.delete_group()


@pytest.mark.slow
def test_imd_restore_from_snapshot_deleting_deltas(workbook, dataset):
    '''
    delete the deltas to make sure restoring from snapshots and not from deltas
    '''
    tab_idx = 0
    imd_tables = [imd_tables_list[tab_idx]]
    group_name = "test_group"
    workbook.inactivate()
    session = workbook.activate()
    try:
        dataset_builder.load()
    except Exception as ex:
        pass

    with tempfile.TemporaryDirectory() as back_store:
        # create group
        test_imd_group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)
        assert test_imd_group.name == group_name
        assert test_imd_group._id in test_imd_group.get_last_txn_id()

        # restore without any update, no op
        test_imd_group.restore()

        # add base
        tab1 = "base1"
        dataset_df = Dataflow.create_dataflow_from_dataset(
            session.client,
            dataset,
            project_columns=imd_tables[tab_idx]["schema"]).custom_sort(
                imd_tables_indexes[tab_idx])
        df1 = dataset_df.map(evals_list=[('absInt(0)',
                                          Rankover), ('absInt(1)', Opcode)])
        session.execute_dataflow(df1, table_name=tab1, is_async=False)

        update_1_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1
        })
        assert update_1_txn_id == test_imd_group.get_last_txn_id()

        # do snapshot and delete deltas explicitly
        test_imd_group.snapshot()
        delta_path = os.path.join(back_store, "delta")
        shutil.rmtree(delta_path)    # deleting the deltas

        # add delta
        df2 = dataset_df.map(
            evals_list=[('add(float_val, 1)',
                         'float_val'), ('absInt(1)',
                                        Rankover), ('absInt(1)', Opcode)])
        tab1_delta = "delta1_tab1"
        session.execute_dataflow(df2, table_name=tab1_delta, is_async=False)
        update_2_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1_delta
        })
        assert update_2_txn_id == test_imd_group.get_last_txn_id()

        # add another delta and do snapshot to see if data exists
        df3 = dataset_df.map(evals_list=[(
            'add(float_val, 1)',
            'float_val'), ('absInt(1)',
                           Rankover), ('absInt(3)',
                                       Opcode)]).filter('gt(id1, 500)')
        tab1_delta = "delta1_tab2"
        session.execute_dataflow(df3, table_name=tab1_delta, is_async=False)
        update_2_txn_id = test_imd_group.merge({
            imd_tables[0]["table_name"]: tab1_delta
        })
        assert update_2_txn_id == test_imd_group.get_last_txn_id()

        # do snapshot and delete deltas explicitly
        test_imd_group.snapshot()
        shutil.rmtree(delta_path)    # deleting the deltas

        # convert source to pandas before you deactivate
        source_pd = pd.DataFrame.from_dict(
            test_imd_group.get_table(imd_tables[0]["table_name"]).records())
        _check_restore_with_source(workbook, test_imd_group, [source_pd],
                                   [imd_tables[0]["table_name"]], ['id1'])

        test_imd_group.delete_group()

        files = os.listdir(back_store)
        assert len(files) == 1    # should just contain deltas and snapshot dir
        # deleted deltas above
        with pytest.raises(FileNotFoundError) as ex:
            delta_files = os.listdir(os.path.join(back_store, "delta"))
        snapshot_files = os.listdir(os.path.join(back_store, "snapshot"))
        assert len(snapshot_files) == 0


# basic tests
'''
1) create imd_table_group with two tables
    tab1 => id1, name, float_val => p_key:id1
    tab2 => id2, id2_1, int_val => p_key: id2, id2_1
2) check kvstore entries exists
3) create base tables and run update on group
4) check kvstore entries updated
5) update tab1 and check results are correct algorithmically
6) check kvstore entries are correct
7) update tab2 with improper index and expect to fail
8) check kvstore entries are correct
9) update tab1, tab2 and check results are correct algorithmically
    also check file paths for tab_1 and tab_2 updates
10) check kvstore entries are correct
11) call get_imd_table_group
12) check kvstore not updated
13) destroy session => shall we update the kvstore state??
14) restore to the latest point
15) verify results
16) update tab_1 and tab_2 and check results
17) check kvstore
18) Remove a table
19) check kvstore
20) session destroy
21) restore to latest and see if table is removed
22) do another update
23) delete the group
24) see if kvstore meta is cleared
'''


def test_imd_basic(client, workbook, dataset):
    # [START imd_table_group_demo]
    workbook.inactivate()
    session = workbook.activate()
    try:
        dataset_builder.load()
    except Exception as ex:
        pass
    imd_tables = imd_tables_list[:2]
    group_name = "test_group"

    with tempfile.TemporaryDirectory() as back_store:
        # create group
        group = session.create_imd_table_group(
            group_name=group_name,
            imd_tables=imd_tables,
            path_to_back_store=back_store)

        # create and add base table
        tab1 = "base1"
        tab2 = "base2"
        tab1_pkeys = []
        for key in imd_tables[0]["primary_keys"]:
            tab1_pkeys.append({
                "name": key,
                "ordering": XcalarOrderingT.XcalarOrderingPartialAscending
            })
        df1 = Dataflow.create_dataflow_from_dataset(
            client, dataset,
            project_columns=imd_tables[0]["schema"]).custom_sort(tab1_pkeys)
        df1 = df1.map(evals_list=[('absInt(1)', Rankover), ('absInt(1)',
                                                            Opcode)])
        tab2_pkeys = []
        for key in imd_tables[1]["primary_keys"]:
            tab2_pkeys.append({
                "name": key,
                "ordering": XcalarOrderingT.XcalarOrderingPartialAscending
            })
        df2 = Dataflow.create_dataflow_from_dataset(
            client, dataset,
            project_columns=imd_tables[1]["schema"]).custom_sort(tab2_pkeys)
        df2 = df2.map(evals_list=[('absInt(1)', Rankover), ('absInt(1)',
                                                            Opcode)])
        session.execute_dataflow(df1, table_name=tab1, is_async=False)
        session.execute_dataflow(df2, table_name=tab2, is_async=False)

        txn_id = group.merge({
            imd_tables[0]["table_name"]: tab1,
            imd_tables[1]["table_name"]: tab2,
        })
        assert txn_id == group.get_last_txn_id()
        assert group.get_table(
            imd_tables[0]["table_name"]).record_count() == num_dataset_rows
        assert group.get_table(
            imd_tables[1]["table_name"]).record_count() == num_dataset_rows
        assert group.info_[ImdConstant.Current_txn_counter] == 2

        # create delta_table1 and update imd_tab1 by add float_val by 1
        df3 = Dataflow.create_dataflow_from_table(
            client, session.get_table(tab1)).map(
                evals_list=[('add(float_val, 1)',
                             'float_val'), ('absInt(1)',
                                            Rankover), ('absInt(1)', Opcode)])
        tab1_delta = "delta1_tab1"
        session.execute_dataflow(df3, table_name=tab1_delta, is_async=False)
        txn_id = group.merge({imd_tables[0]["table_name"]: tab1_delta})
        assert txn_id == group.get_last_txn_id()
        merge_table = group.get_table(imd_tables[0]["table_name"])
        verify_schema(merge_table, imd_tables[0]["schema"])
        for col in merge_table.records():
            assert col['id1'] * 1.0 + 1 == col['float_val']
        assert group.info_[ImdConstant.Current_txn_counter] == 3

        # create delta_table2 and update imd_tab2 with wrong schema
        df4 = Dataflow.create_dataflow_from_table(
            client, session.get_table(tab2)).map(
                evals_list=[('addInteger(int_val, 1)',
                             'extra_column'), ('absInt(1)',
                                               Rankover), ('absInt(1)',
                                                           Opcode)])
        tab2_delta_wrong_schema = "delta2_tab2_wrong_schema"
        session.execute_dataflow(
            df4, table_name=tab2_delta_wrong_schema, is_async=False)
        with pytest.raises(IMDTableSchemaException) as e:
            group.merge({imd_tables[1]["table_name"]: tab2_delta_wrong_schema})
        assert "Schema of delta table" in str(e)
        # make sure the txn id doesn't change
        assert txn_id == group.get_last_txn_id()
        # verify the kvstore meta info
        assert group.info_[ImdConstant.Current_txn_counter] == 4
        assert group._store._get_txn_by_counter(
            group_id=group._id, group_counter=4)["state"] == "FAILED"

        # create delta_table to update imd_tab1 and imd_tab2
        df5 = Dataflow.create_dataflow_from_table(
            client, session.get_table(tab1)).map(evals_list=[(
                'absInt(1)', Rankover), ('absInt(0)', Opcode)]).filter(
                    filter_str="ge(id1, {})".format(num_dataset_rows // 2))
        df6 = Dataflow.create_dataflow_from_table(
            client, session.get_table(tab2)).map(
                evals_list=[('addInteger(int_val, 1)',
                             'int_val'), ('absInt(1)',
                                          Rankover), ('absInt(1)', Opcode)])
        tab1_delta_delete = "delta1_tab1_delete"
        tab2_delta_upsert = "delta2_tab2_upsert"
        session.execute_dataflow(
            df5, table_name=tab1_delta_delete, is_async=False)
        session.execute_dataflow(
            df6, table_name=tab2_delta_upsert, is_async=False)
        txn_id = group.merge({
            imd_tables[0]["table_name"]: tab1_delta_delete,
            imd_tables[1]["table_name"]: tab2_delta_upsert,
        })
        merge_table1 = group.get_table(imd_tables[0]['table_name'])
        merge_table2 = group.get_table(imd_tables[1]['table_name'])

        # verify update result
        assert merge_table1.record_count() == num_dataset_rows // 2
        assert merge_table2.record_count() == num_dataset_rows
        verify_schema(merge_table1, imd_tables[0]["schema"])
        verify_schema(merge_table2, imd_tables[1]["schema"])
        for col in merge_table2.records():
            assert col['id2'] + 1 == col['int_val']
        filter_table_name = session.execute_sql(
            "select 1 from {} where id1 >= {}".format(merge_table1.name,
                                                      num_dataset_rows // 2))
        filter_table = session.get_table(filter_table_name)
        assert filter_table.record_count() == 0

        # verify kvstore meta info
        assert txn_id == group.get_last_txn_id()
        assert group.info_[ImdConstant.Current_txn_counter] == 5
        assert group._store._get_txn_by_counter(
            group_id=group._id, group_counter=5)["state"] == "COMMITTED"
        table_ids = group.info_["table_ids"]
        txn_detail = group._store._get_txn_by_counter(
            group_id=group._id, group_counter=5)['details']
        assert txn_detail['action'] == "MERGE"
        assert set(txn_detail['table_ids']) == set(table_ids)

        group.delete_group()

        files = os.listdir(back_store)
        assert len(files) == 1    # should just contain deltas and snapshot dir
        delta_files = os.listdir(os.path.join(back_store, "delta"))
        assert len(delta_files) == 0

    # [END imd_table_group_demo]


def _check_restore_with_source(workbook, imd_group, source_pds,
                               imd_table_names, p_keys):
    session = workbook.activate()
    validate_imd_group(session, imd_group, imd_group.name, imd_table_names)
    # deactivate workbook and reactivate
    workbook.inactivate(
    )    # XXX ideally should do session.destroy, but it is broken currently
    session = workbook.activate()
    try:
        dataset_builder.load()
    except Exception as ex:
        pass

    imd_group = session.get_imd_table_group(group_name=imd_group.name)
    validate_imd_group(session, imd_group, imd_group.name, imd_table_names)

    # check no data exists
    for imd_table_name in imd_table_names:
        try:
            imd_group.get_table(imd_table_name)
            assert False
        except ValueError as ex:
            if 'No such table:' in str(ex):
                pass

    imd_group.restore()
    validate_imd_group(session, imd_group, imd_group.name, imd_table_names)

    for source_pd, imd_table_name in zip(source_pds, imd_table_names):
        restore_pd = pd.DataFrame.from_dict(
            imd_group.get_table(imd_table_name).records())
        # sort columns before comparing
        source_pd.sort_index(axis=1, inplace=True)
        restore_pd.sort_index(axis=1, inplace=True)
        assert source_pd.shape == restore_pd.shape
        assert (source_pd.columns.values == restore_pd.columns.values).all()
        if source_pd.shape[0] != 0:
            assert ((source_pd.sort_values(by=p_keys).reset_index(
                drop=True) == restore_pd.sort_values(by=p_keys).reset_index(
                    drop=True)).all()).all()


def verify_schema(table, table_schema):
    assert set([x['name'] for x in table_schema]) == set(table.columns)


def validate_imd_locks(imd_store, group_id):
    unlock_val = "UNLOCKED"
    keys = [
        ImdConstant.Txn_lock_key.format(group_id=group_id),
        ImdConstant.Snapshot_lock_key.format(group_id=group_id)
    ]

    for key in keys:
        try:
            assert imd_store.kvstore.lookup(key) == unlock_val
        except XDPException as ex:
            if 'failed to lookup key' in str(ex):
                # it is ok lock not to exists
                pass


def validate_imd_group(session,
                       imd_group,
                       group_name,
                       table_names,
                       path_to_backstore=None,
                       target_name=None):
    test_imd_store = xcalar.external.imd.IMDMetaStore(session)

    assert imd_group._id in imd_group.get_last_txn_id()

    # check group info/details are correct
    # check id name mapping
    group_id = test_imd_store.get_group_id(group_name)
    assert imd_group._id == group_id

    # check locks are ok
    validate_imd_locks(test_imd_store, group_id)

    # validate group info
    test_group_info = test_imd_store.get_group_info(group_name=group_name)
    assert path_to_backstore is None or test_group_info[
        ImdConstant.Backing_store_path] == path_to_backstore
    assert target_name is None or test_group_info[ImdConstant.
                                                  Target_name] == target_name
    assert test_group_info[ImdConstant.Current_txn_counter] >= test_group_info[
        ImdConstant.Last_committed_txn_counter]
    test_table_ids = test_group_info[ImdConstant.Table_ids]
    assert test_group_info[ImdConstant.Group_state] in [
        ImdConstant.IMDGroupState.NEW.name,
        ImdConstant.IMDGroupState.IN_USE.name
    ]
    assert test_group_info[ImdConstant.Group_name] == group_name.upper()

    # validate group transaction details
    all_group_txns_list = test_imd_store.kvstore.list(
        ImdConstant.Group_detail_key.format(
            group_id=group_id, group_counter='.*'))
    assert len(all_group_txns_list) == test_group_info[ImdConstant.
                                                       Current_txn_counter]
    for counter in range(1,
                         test_group_info[ImdConstant.Current_txn_counter] + 1):
        validate_imd_transaction(test_imd_store, group_id, counter)

    # Tables info/detail are correct
    store_table_ids = {
        test_imd_store._get_table_id(table_name): table_name
        for table_name in table_names
    }

    assert sorted(test_table_ids) == sorted(store_table_ids.keys())
    for table_id in test_table_ids:
        validate_imd_table(session, test_imd_store, group_id, table_id,
                           store_table_ids[table_id],
                           test_imd_store._get_table_info(table_id))

    # check any snapshots and all the state is correct
    validate_group_snapshots(group_id, test_imd_store)


def validate_imd_table(session, test_imd_store, group_id, table_id, table_name,
                       table_info):

    # check schema exists
    schema_info = test_imd_store._get_table_schema(
        table_id=table_id,
        schema_version=table_info[ImdConstant.Latest_schema_version])
    schema_txn = test_imd_store._get_txn_by_counter(
        group_id=group_id,
        group_counter=table_info[ImdConstant.Latest_schema_version])
    assert schema_info[ImdConstant.Txn_id] == schema_txn[ImdConstant.Txn_id]
    assert schema_info[ImdConstant.Table_name] == table_name.upper()

    # check table info has all fields
    assert table_info[ImdConstant.Group_id] == group_id
    assert table_info[ImdConstant.Last_committed_txn_counter] > 0
    assert table_info[ImdConstant.Latest_schema_version] > 0
    assert table_info[ImdConstant.Table_name] == table_name.upper()
    assert table_info[ImdConstant.Back_xcalar_table]

    # check back table exists
    is_group_active = test_imd_store._check_group_active(group_id=group_id)
    if is_group_active:
        session.get_table(table_info[ImdConstant.Back_xcalar_table])


def validate_imd_transaction(imd_store, group_id, counter):
    txn_detail = imd_store._get_txn_by_counter(
        group_id=group_id, group_counter=counter)
    txn_id = txn_detail[ImdConstant.Txn_id]
    action = txn_detail[ImdConstant.Details][ImdConstant.Action]
    if action in [ImdConstant.IMDApi.NEW.name, ImdConstant.IMDApi.MERGE.name]:
        for table_id in txn_detail[ImdConstant.Details][ImdConstant.Table_ids]:
            if txn_detail[ImdConstant.Details][
                    ImdConstant.Action] == ImdConstant.IMDApi.NEW.name:
                assert counter == 1
                # check schema exists
                schema_info = imd_store._get_table_schema(
                    table_id=table_id, schema_version=1)
                assert schema_info[ImdConstant.Txn_id] == txn_id
            else:
                table_delta = imd_store._get_table_delta_by_counter(
                    table_id=table_id, group_counter=counter)
                df_delta = imd_store._get_df_delta_by_counter(
                    table_id=table_id, group_counter=counter)
                # we should have either a data checksum or dataflow
                assert table_delta is not None or df_delta is not None
    elif action == ImdConstant.IMDApi.RESTORE.name:
        restore_counter = txn_detail[ImdConstant.Details][
            ImdConstant.Restore_from_txn_counter]
        assert restore_counter > 1 and restore_counter < counter
        restore_txn = imd_store._get_txn_by_counter(
            group_id=group_id, group_counter=restore_counter)
        assert restore_txn[ImdConstant.
                           Txn_state] == ImdConstant.IMDTxnState.COMMITTED.name
        restore_action = restore_txn[ImdConstant.Details][ImdConstant.Action]
        assert restore_action in [
            ImdConstant.IMDApi.MERGE.name, ImdConstant.IMDApi.ALTER.name
        ]
    elif action == ImdConstant.IMDApi.ALTER.name:
        assert False, "Alter not supported yet"
    else:
        assert False, "Invalid action {} found in the txn".format(
            txn_detail[ImdConstant.Details][ImdConstant.Action])


def validate_group_snapshots(group_id, imd_store):
    all_snapshot_list = imd_store.kvstore.list(
        ImdConstant.Snapshot_detail_key.format(
            group_id=group_id, group_counter='.*'))
    all_snapshots = imd_store._get_snapshots(group_id=group_id)
    assert len(all_snapshots) == len(all_snapshot_list)
    group_info = imd_store._get_group_info_by_id(group_id=group_id)

    last_commited_snapshot = None
    for snapshot in all_snapshots:
        assert snapshot[ImdConstant.Start_ts] <= snapshot[ImdConstant.End_ts]

        if snapshot[ImdConstant.
                    Txn_state] == ImdConstant.IMDTxnState.COMMITTED.name:
            last_commited_snapshot = snapshot
            assert len(snapshot[ImdConstant.Details]) > 0
            for info in snapshot[ImdConstant.Details]:
                assert info[ImdConstant.Table_id] in group_info[ImdConstant.
                                                                Table_ids]
                # XXX incremental is not supported currently
                # assert info[ImdConstant.Increment_from] < snapshot[
                #     ImdConstant.Group_counter]
                assert info[ImdConstant.Incremental] is False
                assert info[ImdConstant.Snapshot_checksum] != ""

    last_snapshot = imd_store._get_latest_snapshot(group_id=group_id)
    if last_snapshot is None:
        assert last_commited_snapshot is None
    else:
        assert last_commited_snapshot[ImdConstant.Txn_id] == last_snapshot[
            ImdConstant.Txn_id]


def verify_table_equal(t1, t2):
    if t1.record_count() != t2.record_count():
        return False
    return verify_table_records(t1.records(), t2.records())


def verify_table_records(records1, records2):
    return set(tuple(sorted(x.items())) for x in records1) == set(
        tuple(sorted(x.items())) for x in records2)
