import pytest
import time

from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT
from xcalar.external.exceptions import XDPException
from xcalar.external.dataflow import Dataflow
from xcalar.external.LegacyApi.XcalarApi import XcalarApi as LegacyApi

pytestmark = pytest.mark.usefixtures("config_backup")

imd_tables_list = []
imd_tables_list.append({
    "table_name":
        "imd_tab1",
    "primary_keys": ['id1'],
    "schema": [{
        "name": "id1",
        "type": "Integer"
    }, {
        "name": "float_val",
        "type": "Float"
    }]
})
imd_tables_list.append({
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
})
imd_tables_list.append({
    "table_name":
        "imd_tab3",
    "primary_keys": ["pk1", "pk2"],
    "schema": [{
        "name": "pk1",
        "type": "INTEGER"
    }, {
        "name": "pk2",
        "type": "INTEGER"
    }]
})

num_dataset_rows = 1000
source_udf = """
import json

def gen_dataset(filepath, instream):
    inObj = json.loads(instream.read())
    if inObj["numRows"] == 0:
        return
    start = inObj["startRow"]
    end = start + inObj["numRows"]

    while start < end:
        yield {{"id1": start, "id2": start, "id2_1": start+1, "float_val": start*1.0, "int_val": start}}
        start += 1
""".format(int(time.time()))

dataset_builder = None


@pytest.fixture(scope="module")
def xc_api(workbook):
    xc_api = LegacyApi()
    xc_api.setSession(workbook)
    yield xc_api


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


def table_compare(table1, table2):
    count1 = 0
    count2 = 0
    for r1, r2 in zip(table1.records(), table2.records()):
        count1 += r1["id1"]
        count2 += r2["id1"]
    assert count1 == count2


def test_imd_merge(xc_api, session, dataset):
    tab1 = "imd_base_1"
    delta_tab1 = "imd_delta_1"
    p_keys = [{
        "name": "id1",
        "ordering": XcalarOrderingT.XcalarOrderingPartialAscending
    }]

    df1 = Dataflow.create_dataflow_from_dataset(
        session.client, dataset,
        project_columns=imd_tables_list[0]["schema"]).custom_sort(p_keys)
    session.execute_dataflow(df1, table_name=tab1, is_async=False)
    base_tab1_obj = session.get_table(tab1)
    base_tab1_obj.show()
    with pytest.raises(XDPException) as e:
        base_tab1_obj.merge(base_tab1_obj)
        assert e.statusCode == StatusT.StatusInval

    # Add delta for upserts
    df2 = Dataflow.create_dataflow_from_table(
        session.client, base_tab1_obj).map(evals_list=[(
            'add(float_val, 1)',
            'float_val'), ('absInt(1)',
                           'XcalarOpCode'), ('absInt(1)', "XcalarRankOver")])
    session.execute_dataflow(df2, table_name=delta_tab1, is_async=False)
    delta_tab1_obj = session.get_table(delta_tab1)

    # Add delta for deletes
    delta_tab2 = "imd_delta_2"
    df2 = Dataflow.create_dataflow_from_table(
        session.client, base_tab1_obj).map(evals_list=[(
            'add(float_val, 1)',
            'float_val'), ('absInt(0)',
                           'XcalarOpCode'), ('absInt(2)', "XcalarRankOver")])
    session.execute_dataflow(df2, table_name=delta_tab2, is_async=False)
    delta_tab2_obj = session.get_table(delta_tab2)

    # Force xcalar demand paging
    xc_api.setConfigParam("XcalarPagingThresholdPct", "100")

    # do upserts and deletes
    for x in range(3):
        base_tab1_obj.merge(delta_tab1_obj)
        assert base_tab1_obj.record_count() == num_dataset_rows
        count1 = 0
        count2 = 0
        for r1, r2 in zip(base_tab1_obj.records(), delta_tab1_obj.records()):
            count1 += r1["id1"]
            count2 += r2["id1"]
        assert count1 == count2
        base_tab1_obj.merge(delta_tab2_obj)
        assert base_tab1_obj.record_count() == 0

    # do upserts again
    base_tab1_obj.merge(delta_tab1_obj)
    assert base_tab1_obj.record_count() == num_dataset_rows

    # Test IMD prepare failure
    xc_api.setConfigParam("ImdPreparePercentFailure", "100")
    with pytest.raises(XDPException) as e:
        base_tab1_obj.merge(delta_tab1_obj)
        assert e.status == StatusT.StatusFaultInjection
    table_compare(base_tab1_obj, delta_tab1_obj)

    # Test IMD prepare and abort failure
    xc_api.setConfigParam("ImdPreparePercentFailure", "100")
    xc_api.setConfigParam("ImdAbortPercentFailure", "100")
    with pytest.raises(XDPException) as e:
        base_tab1_obj.merge(delta_tab1_obj)
        assert e.status == StatusT.StatusFaultInjection
    table_compare(base_tab1_obj, delta_tab1_obj)

    # Test IMD commit failure
    xc_api.setConfigParam("ImdPreparePercentFailure", "0")
    xc_api.setConfigParam("ImdCommitPercentFailure", "100")
    xc_api.setConfigParam("ImdAbortPercentFailure", "0")
    with pytest.raises(XDPException) as e:
        base_tab1_obj.merge(delta_tab1_obj)
        assert e.status == StatusT.StatusFaultInjection
    table_compare(base_tab1_obj, delta_tab1_obj)

    # Test IMD commit and abort failure
    xc_api.setConfigParam("ImdCommitPercentFailure", "100")
    xc_api.setConfigParam("ImdAbortPercentFailure", "100")
    with pytest.raises(XDPException) as e:
        base_tab1_obj.merge(delta_tab1_obj)
        assert e.status == StatusT.StatusFaultInjection
    table_compare(base_tab1_obj, delta_tab1_obj)

    # Test IMD post commit failure
    xc_api.setConfigParam("ImdCommitPercentFailure", "0")
    xc_api.setConfigParam("ImdPostCommitPercentFailure", "100")
    xc_api.setConfigParam("ImdAbortPercentFailure", "0")
    base_tab1_obj.merge(delta_tab1_obj)
    xc_api.setConfigParam("ImdPostCommitPercentFailure", "0")
    table_compare(base_tab1_obj, delta_tab1_obj)

    # Revert all error injections
    xc_api.setConfigParam("ImdPreparePercentFailure", "0")
    xc_api.setConfigParam("ImdCommitPercentFailure", "0")
    xc_api.setConfigParam("ImdPostCommitPercentFailure", "0")
    xc_api.setConfigParam("ImdAbortPercentFailure", "0")

    # do upserts and deletes
    for x in range(3):
        base_tab1_obj.merge(delta_tab1_obj)
        assert base_tab1_obj.record_count() == num_dataset_rows
        count1 = 0
        count2 = 0
        for r1, r2 in zip(base_tab1_obj.records(), delta_tab1_obj.records()):
            count1 += r1["id1"]
            count2 += r2["id1"]
        assert count1 == count2
        base_tab1_obj.merge(delta_tab2_obj)
        assert base_tab1_obj.record_count() == 0

    # do upserts again
    base_tab1_obj.merge(delta_tab1_obj)
    assert base_tab1_obj.record_count() == num_dataset_rows

    # create new delta table to delete
    delta_tab3 = "imd_delta_3"
    df2 = Dataflow.create_dataflow_from_table(
        session.client, base_tab1_obj).map(evals_list=[(
            'add(float_val, 1)',
            'float_val'), ('absInt(0)',
                           'XcalarOpCode'), ('absInt(1)', "XcalarRankOver")])
    session.execute_dataflow(df2, table_name=delta_tab3, is_async=False)
    delta_tab3_obj = session.get_table(delta_tab3)

    # delete all records
    base_tab1_obj.merge(delta_tab3_obj)
    assert base_tab1_obj.record_count() == 0
    base_tab1_obj.drop()
