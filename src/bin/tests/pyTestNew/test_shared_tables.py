from xcalar.compute.util.Qa import DefaultTargetName, buildNativePath
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.external.exceptions import XDPException
from xcalar.external.dataflow import Dataflow
import xcalar.external.table
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.compute.util.query_gen_util import QueryGenHelper

import pytest


@pytest.fixture(scope="module")
def session(workbook):
    session = workbook.activate()
    yield session
    session.destroy()


@pytest.fixture(scope="module")
def cross_session(client):
    cross_session = client.create_session("TestCrossSessionTables")
    yield cross_session
    cross_session.destroy()


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
def operators(session):
    xc_api = XcalarApi()
    xc_api.setSession(session)
    operators = Operators(xc_api)
    yield operators


@pytest.fixture(scope="module")
def telecom_table(telecom_dataset, session, operators):
    table_name = "telecom test table"
    operators.indexDataset(telecom_dataset.name, table_name, "State", "p")
    table = session.get_table(table_name)
    yield table


def verify_tables_count(session, tables_count, shared_count):
    session_shared_tables = session.list_tables(globally_shared_only=True)
    assert (len(session_shared_tables) == shared_count)
    session_tables = session.list_tables()
    assert (len(session_tables) == tables_count)


def test_table_sharing(client, session, cross_session, telecom_table):
    xc_api = XcalarApi()
    xc_api.setSession(session)
    op = Operators(xc_api)

    df = Dataflow.create_dataflow_from_table(client, telecom_table)
    table_name = "df_table"

    df._query_list[0]['args']['sameSession'] = False    # synthesize operation

    # Create a table
    session.execute_dataflow(
        df, table_name=table_name, is_async=False, optimized=False)
    table_obj = session.get_table(table_name)

    tab_not_exists = xcalar.external.table.Table(session, "foo")
    # Test invalid params
    with pytest.raises(XDPException) as e:
        tab_not_exists.publish()
        assert e.status == StatusT.StatusTableNameNotFound
    with pytest.raises(XDPException) as e:
        tab_not_exists.unpublish()
        assert e.status == StatusT.StatusTableNameNotFound

    # Try publish/unpublish in a loop
    for iter in range(3):
        print("\nIteration: {}".format(iter))
        tables = session.list_tables()
        tcount = len(tables)

        # Verify that all the tables in the session are local only
        verify_tables_count(session, 2, 0)

        tcur = 0
        for t in tables:
            tcur = tcur + 1
            # Promote a table as global and verify that the table is in global Namespace
            t.publish()
            verify_tables_count(session, tcount, tcur)

        tcur = 0
        for t in tables:
            tcur = tcur + 1
            # Depromote a table as local and verify that the table is NOT in global Namespace
            t.unpublish()
            verify_tables_count(session, tcount, tcount - tcur)

    # Publish a table into global namespace and list global tables across sessions
    print("\nCross session Global table list")
    table_obj = session.get_table(table_name)
    table_obj.publish()
    verify_tables_count(session, 2, 1)
    # verify_tables_count(cross_session, 0, 1) ???

    # Run a DF from cross_session againt table published
    print("\nCross session Global table operation")
    shared_tables = session.list_tables(globally_shared_only=True)
    assert (len(shared_tables) == 1)
    fq_table_obj = shared_tables[0]

    cross_table_name = "CrossTableName"
    xc_cross_api = XcalarApi()
    xc_cross_api.setSession(cross_session)
    op_cross = Operators(xc_cross_api)
    print("\tRun operator Map src '{}' dst '{}' sess '{}'".format(
        fq_table_obj.fqn_name, cross_table_name, cross_session.name))
    op_cross.map(fq_table_obj.fqn_name, cross_table_name, ["int(1)"],
                 ["IntColumn"])
    assert len(
        cross_session.list_tables()) == 1    # new table in cross session
    print("\tPublish '{}' as global".format(cross_table_name))
    cross_table_obj = cross_session.list_tables()[0]
    assert cross_table_obj.record_count() == telecom_table.record_count()
    verify_tables_count(cross_session, 1, 0)
    cross_table_obj.publish()
    assert cross_table_obj.record_count() == telecom_table.record_count()
    verify_tables_count(cross_session, 1, 1)
    cross_table_obj.unpublish()
    verify_tables_count(cross_session, 1, 0)

    # Now let's publish a table from cross session and then use in the session
    cross_table_obj.publish()
    fq_table_obj = cross_session.list_tables(globally_shared_only=True)[0]
    print("\tRun operator Map src '{}' dst '{}' wb '{}'".format(
        fq_table_obj.fqn_name, cross_table_name, session.name))
    op.map(fq_table_obj.fqn_name, cross_table_name, ["int(1)"], ["IntColumn"])
    print([tab.name for tab in session.list_tables(globally_shared_only=True)])
    print([tab.name for tab in session.list_tables()])
    verify_tables_count(session, 3, 1)
    new_tab_obj = session.get_table(cross_table_name)
    assert new_tab_obj.record_count() == telecom_table.record_count()

    # Index on Shared Tables
    index_table_name = "idx_table"
    print("\tRun operator Index src '{}' dst '{}' wb '{}'".format(
        fq_table_obj.fqn_name, index_table_name, session.name))
    qgh = QueryGenHelper(client=client, session=session)
    schema = qgh.get_schema(fq_table_obj)
    keys_map = qgh.get_rand_keys_map(schema)
    qgh.issue_index(source=fq_table_obj.fqn_name,
            dest = index_table_name,
            keys_map=keys_map)
    new_tab_obj = session.get_table(index_table_name)
    assert new_tab_obj.record_count() == telecom_table.record_count()

    # GetRowNum on Shared Tables
    row_num_table_name = "row_num_table"
    row_num_col_name = "row_num_column"
    print("\tRun operator row num src '{}' dst '{}' wb '{}'".format(
        fq_table_obj.fqn_name, row_num_table_name, session.name))
    qgh = QueryGenHelper(client=client, session=session)
    qgh.issue_row_num(
            source=fq_table_obj.fqn_name,
            dest=row_num_table_name,
            new_field=row_num_col_name)
    new_tab_obj = session.get_table(row_num_table_name)
    assert new_tab_obj.record_count() == telecom_table.record_count()

    # Cross session table resultSet
    print("\nResultSet table '{}'".format(fq_table_obj.fqn_name))
    fq_table_obj = xcalar.external.table.Table(session, fq_table_obj.fqn_name)
    fq_table_obj.publish()
    fq_table_obj.show()
    print("Table '{}' Meta '{}'".format(fq_table_obj.fqn_name,
                                        fq_table_obj.get_meta()))
    print("\nResultSet table '{}'".format(fq_table_obj.fqn_name))
    fq_table_obj = xcalar.external.table.Table(cross_session,
                                               fq_table_obj.fqn_name)
    fq_table_obj.show()
    print("Table '{}' Meta '{}'".format(fq_table_obj.fqn_name,
                                        fq_table_obj.get_meta()))

    # Construct a dataflow from global table
    df = Dataflow.create_dataflow_from_table(client, fq_table_obj)
    df._query_list[0]['args'][
        'source'] = fq_table_obj.fqn_name    # make source FQN
    cross_session.execute_dataflow(
        df, table_name="foo", is_async=False, optimized=True)

    # Enable once non-optimized execution works for shared tables
    df = Dataflow.create_dataflow_from_table(client, fq_table_obj)
    df._query_list[0]['args']['source'] = fq_table_obj.fqn_name  # make source FQN
    cross_session.execute_dataflow(df, table_name="bar", is_async=False, optimized=False)
    fq_table_obj.drop(delete_completely=True)


def test_global_tables(client, session, cross_session, telecom_table):
    prev_global_tab_count = len(client.list_global_tables())

    source_df = Dataflow.create_dataflow_from_table(client, telecom_table)
    table_name = "new_global_tab"

    # Create a table
    session.execute_dataflow(
        source_df, table_name=table_name, is_async=False, optimized=True)
    table_obj = session.get_table(table_name)

    with pytest.raises(ValueError) as ex:
        client.get_global_table(table_name)
        assert "No such table: '{}'".format(table_name) in str(ex)

    rec_count_1 = table_obj.record_count()
    table_obj.publish()
    tab = client.get_global_table(table_name)
    assert tab.fqn_name == table_obj.fqn_name
    assert tab.record_count() == rec_count_1

    tabs = client.list_global_tables("new_global*")
    assert len(tabs) == 1

    assert len(client.list_global_tables("NO_PATTERN_MATCH*")) == 0

    # Construct a dataflow from global table
    df = Dataflow.create_dataflow_from_table(client, table_obj)
    df._query_list[0]['args'][
        'source'] = table_obj.fqn_name    # make source FQN
    cross_session.execute_dataflow(
        df, table_name="temp1", is_async=False, optimized=True)
    table_obj.drop(delete_completely=True)

    cross_session.get_table("temp1").drop(delete_completely=True)

    # Let's create a global tab again
    session.execute_dataflow(
        source_df, table_name=table_name, is_async=False, optimized=True)
    table_obj = session.get_table(table_name)
    table_obj.publish()

    df._query_list[0]['args'][
        'source'] = table_obj.fqn_name    # make source FQN
    cross_session.execute_dataflow(
        df, table_name="temp1", is_async=False, optimized=True)

    # Run the same DF to expose StatusTableAlreadyExists case
    with pytest.raises(XcalarApiStatusException) as e:
        cross_session.execute_dataflow(
            df, table_name="temp1", is_async=False, optimized=True)
        assert e.status == StatusT.StatusTableAlreadyExists
    with pytest.raises(XcalarApiStatusException) as e:
        cross_session.execute_dataflow(
            df, table_name="temp1", is_async=False, optimized=True)
        assert e.status == StatusT.StatusTableAlreadyExists

    # This succeeds, since the table name is unique now
    df = Dataflow.create_dataflow_from_table(client, table_obj)
    df._query_list[0]['args'][
        'source'] = table_obj.fqn_name    # make source FQN
    cross_session.execute_dataflow(
        df, table_name="temp2", is_async=False, optimized=True)

    # This fails bc table is now unpublished
    table_obj.unpublish()

    # XXX TODO ENG-9062 Cannot enforce test for global Tables bc of Synthesize
    # sameSession.
    #with pytest.raises(XcalarApiStatusException) as e:
    #    df = Dataflow.create_dataflow_from_table(client, table_obj)
    #    df._query_list[0]['args'][
    #        'source'] = table_obj.fqn_name    # make source FQN
    #    cross_session.execute_dataflow(
    #        df, table_name="temp3", is_async=False, optimized=True)
    #    assert e.status == StatusT.StatusTableNotGlobal
