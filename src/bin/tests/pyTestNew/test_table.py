import random

from xcalar.compute.util.Qa import DefaultTargetName, buildNativePath
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException

from xcalar.compute.coretypes.Status.ttypes import StatusT

from xcalar.external.exceptions import XDPException
from xcalar.external.dataflow import Dataflow

import pytest


@pytest.fixture(scope="module")
def session(workbook):
    session = workbook.activate()
    yield session
    session.destroy()


@pytest.fixture(scope="module")
def telecom_dataset(client, workbook):
    dataset_name = "TableAPIsDS"
    file_path = "exportTests/telecom.csv"
    path = buildNativePath(file_path)

    data_target = client.get_data_target(DefaultTargetName)
    dataset_builder = workbook.build_dataset(dataset_name, data_target, path,
                                             "csv")
    dataset_builder.option("fieldDelim", ",").option("schemaMode", "header")
    dataset = dataset_builder.load()
    yield dataset
    dataset.delete()


@pytest.fixture(scope="module")
def telecom_table(telecom_dataset, workbook, operators):
    table_name = "telecom test table"
    operators.indexDataset(telecom_dataset.name, table_name, "State", "p")
    session = workbook.activate()
    table = session.get_table(table_name)
    yield table


@pytest.fixture(scope="module")
def operators(workbook):
    xc_api = XcalarApi()
    xc_api.setSession(workbook)

    operators = Operators(xc_api)

    yield operators


def test_add_list_delete(workbook, telecom_dataset, operators):
    table_name = "hello table"
    session = workbook.activate()
    original_tables = set(session.list_tables())
    assert table_name not in (t.name for t in session.list_tables())

    operators.indexDataset(telecom_dataset.name, table_name, "State", "p")
    assert original_tables | set([table_name]) == set(
        t.name for t in session.list_tables())

    table = session.get_table(table_name)
    assert table.name == table_name

    table_name_del_complete = "del completely"
    operators.indexTable(table.name, table_name_del_complete,
                         "p::Phone Number")
    table.drop()
    original_tables.add(table_name_del_complete)
    assert original_tables == set(t.name for t in session.list_tables())

    # Exercise table drop with delete_completely=True
    table = session.get_table(table_name_del_complete)
    table.drop(delete_completely=True)
    original_tables.remove(table_name_del_complete)
    assert original_tables == set(t.name for t in session.list_tables())


def test_result_set(telecom_table):
    # Make sure records is an iterator
    expected_num_records = 5000
    all_records = list(telecom_table.records())
    assert len(
        all_records) == telecom_table.record_count() == expected_num_records

    # Let's spot check some of the rows. We don't want to test all of them
    # because it would be slow.
    random.seed(13378337)
    num_checks = 100
    checked_indecies = [
        random.randint(0, expected_num_records) for _ in range(num_checks)
    ]
    for index in checked_indecies:
        assert all_records[index] == telecom_table.get_row(index)


def test_table_meta(telecom_table):
    tab_meta = telecom_table._get_meta()
    schema = telecom_table.schema
    columns = telecom_table.columns

    meta_dict = tab_meta.as_json_dict()

    # fetch all properties
    assert (tab_meta.table_name == meta_dict['attributes']['table_name'])
    assert (tab_meta.table_id == int(meta_dict['attributes']['table_id']))
    assert (tab_meta.pinned == meta_dict['attributes']['pinned'])
    assert (tab_meta.shared == meta_dict['attributes']['shared'])
    assert (tab_meta.schema == schema)
    assert (tab_meta.columns == columns)

    assert (len(tab_meta.keys) == len(meta_dict['schema']['key_attributes']))
    for key_info in meta_dict['schema']['key_attributes']:
        assert (key_info['name'] in tab_meta.keys)
        assert (key_info['type'] == tab_meta.keys.get(
            key_info['name'])['type'])

    assert (len(tab_meta.columns) == len(tab_meta.schema) == len(
        meta_dict['schema']['column_attributes']))
    for col_info in meta_dict['schema']['column_attributes']:
        assert (col_info['name'] in tab_meta.columns)
        assert (col_info['type'] == tab_meta.schema.get(col_info['name']))
    assert (tab_meta.state == "Ready")

    # aggregated stats
    assert (tab_meta.total_records_count == int(
        meta_dict['aggregated_stats']['total_records_count']))
    assert (tab_meta.total_size_in_bytes == int(
        meta_dict['aggregated_stats']['total_size_in_bytes']))
    assert (tab_meta.records_count_per_node == list(
        map(int, meta_dict['aggregated_stats']['rows_per_node'])))
    assert (tab_meta.size_per_node == list(
        map(int, meta_dict['aggregated_stats']['size_in_bytes_per_node'])))

    # stats per node
    assert (len(meta_dict['stats_per_node']) == 0)

    meta_dict = telecom_table.get_meta(
        include_per_node_stats=True).as_json_dict()
    assert (len(meta_dict['stats_per_node']['Node-0']) == 11)


# keep this test last as it drops all the tables
def test_pin_unpin(client, workbook, session, telecom_table):
    xc_api = XcalarApi()
    xc_api.setSession(session)

    df = Dataflow.create_dataflow_from_table(client, telecom_table)
    table_name = "df_table"

    df._query_list[0]['args']['sameSession'] = False    # synthesize operation
    # create a table
    session.execute_dataflow(
        df, table_name=table_name, is_async=False, optimized=False)
    xc_tab = session.get_table(table_name)
    assert xc_tab.is_pinned() is False
    xc_tab.drop()
    with pytest.raises(ValueError) as ex:
        session.get_table(table_name)
        assert "No such table: '{}'".format(table_name) in str(ex)

    # create table again
    session.execute_dataflow(
        df, table_name=table_name, is_async=False, optimized=False)
    xc_tab = session.get_table(table_name)
    xc_tab.pin()
    assert xc_tab.is_pinned() is True
    # Check if the table is pinned using thrift call
    table_list_info = xc_api.listTable(tableNamePattern=xc_tab.name)
    assert table_list_info.numNodes == 1
    assert table_list_info.nodeInfo[0].name == xc_tab.name
    assert table_list_info.nodeInfo[0].pinned

    with pytest.raises(XDPException) as ex:
        xc_tab.pin()
        assert ex.statusCode == StatusT.StatusTableAlreadyPinned
    with pytest.raises(XcalarApiStatusException) as ex:
        xc_tab.drop()
        assert ex.status == StatusT.StatusTablePinned
    xc_tab.unpin()
    assert xc_tab.is_pinned() is False
    # Check if the table is unpin using thrift call
    table_list_info = xc_api.listTable(tableNamePattern=xc_tab.name)
    assert table_list_info.numNodes == 1
    assert table_list_info.nodeInfo[0].name == xc_tab.name
    assert table_list_info.nodeInfo[0].pinned is False

    # unpin again should fail
    with pytest.raises(XDPException) as ex:
        xc_tab.unpin()
        assert ex.statusCode == StatusT.StatusTableNotPinned
    assert xc_tab.is_pinned() is False
    xc_tab.drop()
    with pytest.raises(ValueError) as ex:
        session.get_table(table_name)
        assert "No such table: '{}'".format(table_name) in str(ex)

    # pin operations on droped table should fail
    with pytest.raises(XDPException) as ex:
        xc_tab.pin()
        assert ex.statusCode == StatusT.StatusDagNodeNotFound

    with pytest.raises(XDPException) as ex:
        xc_tab.unpin()
        assert ex.statusCode == StatusT.StatusDagNodeNotFound

    with pytest.raises(XDPException) as ex:
        xc_tab.is_pinned()
        assert ex.statusCode == StatusT.StatusDagNodeNotFound

    # pin the table, should allow deactivating the session
    session.execute_dataflow(
        df, table_name=table_name, is_async=False, optimized=False)
    xc_tab = session.get_table(table_name)
    xc_tab.pin()
    session.execute_dataflow(
        df, table_name=table_name + "_tmp", is_async=False, optimized=False)
    session.drop_tables("*", delete_completely=True)
    assert len(session.list_tables()) == 1, len(session.list_tables())
    session.destroy()
    session = workbook.activate()
    with pytest.raises(ValueError) as ex:
        session.get_table(table_name)
        assert "No such table: '{}'".format(table_name) in str(ex)
