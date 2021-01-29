import pytest
import random

from xcalar.compute.util.Qa import DefaultTargetName, buildNativePath

from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.result_set import ResultSet
from xcalar.external.dataflow import Dataflow

from xcalar.compute.coretypes.Status.ttypes import StatusT


@pytest.fixture(scope="module")
def session(workbook):
    session = workbook.activate()
    yield session
    session.destroy()


@pytest.fixture(scope="module")
def telecom_dataset(client, workbook):
    dataset_name = "ResultSetAPIsDS"
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
def telecom_table(telecom_dataset, client, session):
    df = Dataflow.create_dataflow_from_dataset(client, telecom_dataset)
    column_renames = []
    for column in telecom_dataset._summary()['columns']:
        column_rename = {}
        column_rename['name'] = column['name']
        column_rename['rename'] = column['name']
        column_rename['type'] = 'string'
        column_renames.append(column_rename)
    table_name = "synthesized telecom test table"
    df.synthesize(column_renames)
    session.execute_dataflow(df, table_name=table_name, is_async=False)
    table = session.get_table(table_name)
    yield table
    table.drop()


def test_non_existent_dataset(client):
    try:
        rs = ResultSet(client, dataset_name="nonexistent").record_iterator()
    except XcalarApiStatusException as e:
        assert e.status == StatusT.StatusDsNotFound


def test_non_existent_table(client, workbook):
    try:
        rs = ResultSet(
            client, table_name="nonexistent",
            session_name=workbook.name).record_iterator()
    except XcalarApiStatusException as e:
        assert e.status == StatusT.StatusDsNotFound


def test_dataset_resultset(client, telecom_dataset):
    rs = ResultSet(client, dataset_name=telecom_dataset.name)
    for r in rs.record_iterator():
        pass
    rs.get_row(random.randint(0, rs.record_count() - 1))


def test_table_resultset(client, telecom_table):
    rs = ResultSet(
        client,
        table_name=telecom_table.name,
        session_name=telecom_table._session.name)
    for r in rs.record_iterator():
        pass
    rs.get_row(random.randint(0, rs.record_count() - 1))
