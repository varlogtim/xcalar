# Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import pytest
from click.testing import CliRunner
import os
import tempfile
import csv
import subprocess
import random
from socket import gethostname

import xcalar.xc2.main
from xcalar.external.client import Client
from xcalar.external.LegacyApi.XcalarApi import XcalarApi
from xcalar.external.LegacyApi.Operators import Operators
from xcalar.external.runtime import Runtime

from xcalar.compute.util.Qa import XcalarQaDatasetPath, DefaultTargetName, buildNativePath


# parts of this function will be visible in the documentation
def test_download_upload(cluster):
    # [START download_upload]
    # from xcalar.external.client import Client

    client = Client()

    # create a new workbook
    workbook = client.create_workbook(workbook_name="demo")

    # download the newly created workbook
    dl = workbook.download()

    # upload a copy of the same workbook, but with a different name
    uploaded_copy = client.upload_workbook(
        workbook_name="demo_copy", workbook_content=dl)

    # activate newly uploaded copy
    uploaded_copy.activate()

    # delete original workbook
    workbook.delete()
    # [END download_upload]

    # clean up
    uploaded_copy.delete()


# parts of this function will show up in documentation
def test_list_create(cluster):
    # [START list_create]
    # from xcalar.external.client import Client

    client = Client()

    # list_workbooks() should be used to find a workbook if you are
    # unsure it exists. We could use get_workbook(), however, in the
    # case that workbook is not found, get_workbook() raises a
    # XcalarApiStatusException (which would need to be handled by your code).
    # list_workbooks, on the other hand, returns a empty list if search
    # pattern is not found.

    workbooks = client.list_workbooks(pattern="test_workbook")

    if (len(workbooks) == 0):
        # no workbooks were found matching the name, thus new
        # workbook should be created
        desired_workbook = client.create_workbook("test_workbook", active=True)
    else:
        desired_workbook = workbooks[0]

    # [END list_create]
    desired_workbook.delete()


def test_create_list_delete_rename(client):
    test_workbook_name_head = "TestCreateListDeleteRename_"
    init_num_workbooks = len(client.list_workbooks())

    # test create_workbook
    names = []
    for i in range(4):
        name = test_workbook_name_head + str(i)
        names.append(name)
        client.create_workbook(name)

    workbooks = client.list_workbooks()

    assert (len(workbooks) == (len(names) + init_num_workbooks))

    # test get_workbook
    for name in names:
        new_workbook = client.get_workbook(name)
        assert (new_workbook.name == name)

    # should always fail as session already exists
    with pytest.raises(Exception) as e:
        workbook_fail = client.create_workbook(names[0])
    assert (e.value.message == "Session already exists")

    workbook_1 = client.get_workbook(names[0])
    workbook_2 = client.get_workbook(names[1])

    # should always fail as session name already exists
    with pytest.raises(Exception) as e:
        workbook_1.rename(workbook_2.name)
    assert (e.value.message == "Session already exists")

    # test renaming all the workbooks
    for name in names:
        workbook = client.get_workbook(name)
        new_name = name + "_renamed"
        workbook.rename(new_name)
        # check that renamed workbook is there
        client.get_workbook(new_name)
        # make sure old name no longer exists
        assert (len(client.list_workbooks(pattern=name)) == 0)

    workbooks = client.list_workbooks()
    assert (len(workbooks) == (len(names) + init_num_workbooks))

    # test delete
    new_names = [name + "_renamed" for name in names]
    for name in new_names:
        client.get_workbook(name).delete()

    assert (len(client.list_workbooks()) == init_num_workbooks)


def test_workbook_states(client):
    test_workbook_name_head = "TestWorkbookStates_"
    workbook_1 = client.create_workbook(test_workbook_name_head + "1")
    workbook_2 = client.create_workbook(
        test_workbook_name_head + "2", active=False)

    assert (workbook_1.is_active())
    assert (not workbook_2.is_active())

    workbook_1.inactivate()
    assert (not workbook_1.is_active())

    workbook_2.activate()
    assert (workbook_2.is_active())

    workbook_2.persist()

    # this persist should not fail as persist is idempotent
    workbook_1.persist()

    # this activate should not fail as activate is idempotent
    workbook_2.activate()

    # this inactivate should not fail as inactivate is idempotent
    workbook_1.inactivate()

    workbook_1.delete()
    workbook_2.delete()


def test_fork(client):
    test_fork_name_head = "TestWorkbookFork_"
    wb = client.create_workbook(test_fork_name_head + "_1")
    forked_name = test_fork_name_head + "_forked"
    forked_wb = wb.fork(forked_name)
    assert (client.get_workbook(forked_name).name == forked_wb.name)

    wb.delete()
    forked_wb.delete()


def test_error_raising(client):
    # the following should always fail client-side due to incorrect types
    with pytest.raises(TypeError) as e:
        client.create_workbook(1)
    assert str(e.value) == "workbook_name must be str, not '{}'".format(
        type(1))

    with pytest.raises(TypeError) as e:
        client.create_workbook("sample", "F")
    assert str(e.value) == "active must be bool, not '{}'".format(type("F"))

    with pytest.raises(TypeError) as e:
        client.upload_workbook(1, workbook_content=None)
    assert str(e.value) == "workbook_name must be str, not '{}'".format(
        type(1))

    no_such_workbook = "InexistantWorkbookNameTest"

    with pytest.raises(TypeError) as e:
        client.get_workbook(1)
    assert str(e.value) == "workbook_name must be str, not '{}'".format(
        type(1))

    # raises value error as workbook does not exist
    with pytest.raises(ValueError) as e:
        client.get_workbook(no_such_workbook)
    assert str(e.value) == "No such workbook: '{}'".format(no_such_workbook)

    rand_wb_name_1 = "RandomWorkbookNameTestErrors_1"
    rand_wb_name_2 = "RandomWorkbookNameTestErrors_2"
    wb1 = client.create_workbook(rand_wb_name_1)
    wb2 = client.create_workbook(rand_wb_name_2)

    with pytest.raises(TypeError) as e:
        wb2.rename(1)
    assert str(e.value) == "new_name must be str, not '{}'".format(type(1))

    with pytest.raises(TypeError) as e:
        wb2.fork(1)
    assert str(e.value) == "workbook_name must be str, not '{}'".format(
        type(1))

    with pytest.raises(TypeError) as e:
        wb2.fork("valid_name", "F")
    assert str(e.value) == "active must be bool, not '{}'".format(type("F"))

    # raises value error as multiple workbooks found
    with pytest.raises(ValueError) as e:
        client.get_workbook("*")
    assert str(e.value) == "Multiple workbooks found matching: '*'"

    wb1.delete()
    wb2.delete()

    # type error
    with pytest.raises(TypeError) as e:
        client.list_workbooks(1)
    assert str(e.value) == "pattern must be str, not '{}'".format(type(1))


def test_invalid_session_names(client):
    with pytest.raises(Exception) as e:
        workbook = client.create_workbook("")
        assert (e.value.message == "Session user name is invalid")

    with pytest.raises(Exception) as e:
        workbook = client.create_workbook(" ")
        assert (e.value.message == "Session user name is invalid")

    with pytest.raises(Exception) as e:
        workbook = client.create_workbook(" noLeadingSpaces")
        assert (e.value.message == "Session user name is invalid")

    with pytest.raises(Exception) as e:
        workbook = client.create_workbook("noTrailingSpaces ")
        assert (e.value.message == "Session user name is invalid")


def test_convert_kvs_to_query(client):
    test_wb_name = "convert_kvs_workbook"
    wb_path = XcalarQaDatasetPath + "/dfWorkbookTests/kvsToQueryTest.xlrdf.tar.gz"

    # clean up old session, if any
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    with open(wb_path, 'rb') as wb_file:
        wb_content = wb_file.read()
        workbook = client.upload_workbook(test_wb_name, wb_content)

        # workbook is not active, XXX: see what all we can do e.g.
        # df_list = workbook.list_dataflows()

        sess = workbook.activate()

        # Load the dataset using the name in the dataflow file.  While some
        # may say that it would be nice to auto-load the dataset this isn't
        # desired per our product vision which says the dataset must be already
        # loaded in the cluster.
        db = workbook.build_dataset(
            dataset_name="admin.89753.airlines_dataset",
            data_target=DefaultTargetName,
            path="/netstore/datasets/XcalarTraining/airlines.csv",
            import_format="csv")
        db.load()

        db2 = workbook.build_dataset(
            dataset_name="admin.72496.carriers_dataset",
            data_target=DefaultTargetName,
            path="/netstore/datasets/XcalarTraining/carriers.json",
            import_format="json")
        db2.load()

        df_names = workbook.list_dataflows()
        assert (len(df_names) == 1)

        df = workbook.get_dataflow(df_names[0])
        params = {'month_val': 8, 'aggr': 'count'}
        sched_name = random.choice(Runtime.get_dataflow_scheds())
        parallel_ops = random.choice([True, False])
        clean_job = random.choice([True, False])
        sess.execute_dataflow(
            df,
            'my_final_table',
            optimized=False,
            params=params,
            is_async=False,
            clean_job_state=clean_job,
            sched_name=sched_name,
            parallel_operations=parallel_ops)
        table = sess.get_table(table_name="my_final_table")
        record = table.get_row(0)
        assert ("Mesa Airlines" in record['Description'])

        cnt = len(list(table.records()))
        assert (cnt == 16)


def test_get_dataflow_sql_parameterized(client):
    test_wb_name = "df_sql_parameterized"
    wb_path = XcalarQaDatasetPath + "/dfWorkbookTests/dfSqlParameterized.xlrdf.tar.gz"

    # clean up old session, if any
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    with open(wb_path, 'rb') as wb_file:
        wb_content = wb_file.read()
        workbook = client.upload_workbook(test_wb_name, wb_content)

    sess = workbook.activate()

    df_names = workbook.list_dataflows()
    assert (len(df_names) == 3)

    # first execute the dataflow with no sql parameterized
    params = {'carrierParam': 'A', 'dayofweekparam': 5}
    df = workbook.get_dataflow("df1")
    sched_name = random.choice(Runtime.get_dataflow_scheds())
    parallel_ops = random.choice([True, False])
    clean_job = random.choice([True, False])
    sess.execute_dataflow(
        df,
        'parametierized_result',
        optimized=True,
        is_async=False,
        clean_job_state=clean_job,
        sched_name=sched_name,
        parallel_operations=parallel_ops)
    table1 = sess.get_table(table_name="parametierized_result")

    # second execute the similar dataflow with sql parameters
    df = workbook.get_dataflow('df2', params=params)
    sched_name = random.choice(Runtime.get_dataflow_scheds())
    parallel_ops = random.choice([True, False])
    clean_job = random.choice([True, False])
    sess.execute_dataflow(
        df,
        'non_parametierized_result',
        optimized=True,
        is_async=False,
        clean_job_state=clean_job,
        sched_name=sched_name,
        parallel_operations=parallel_ops)
    table2 = sess.get_table(table_name="non_parametierized_result")

    # check both got same number of records
    assert table1.record_count() == table2.record_count() == 11024
    # check got all the columns
    assert len(table1.get_row(0)) == len(table2.get_row(0)) == 16

    # run the third dataflow which compares the results of two outputs
    params = {
        'nonParamRes': 'non_parametierized_result',
        'ParamRes': 'parametierized_result'
    }
    df = workbook.get_dataflow('df3')
    sched_name = random.choice(Runtime.get_dataflow_scheds())
    parallel_ops = random.choice([True, False])
    clean_job = random.choice([True, False])
    sess.execute_dataflow(
        df,
        'diff_result',
        params=params,
        optimized=True,
        is_async=False,
        clean_job_state=clean_job,
        sched_name=sched_name,
        parallel_operations=parallel_ops)
    table3 = sess.get_table(table_name="diff_result")
    assert (table3.record_count() == 0)

    # cleanup
    sess.destroy()    # session destroys in the cluster but not client object
    workbook.delete(
    )    # workbook gets deleted in the cluster but not workbook object
    assert len(client.list_sessions(sess.name)) == 0
    assert len(client.list_workbooks(workbook.name)) == 0


def test_run_dataflow_xc2(client):
    test_wb_name = "kvsToQueryTest"
    df_name = ".temp/rand38135/kvsToQueryTest"
    wb_path = XcalarQaDatasetPath + "/dfWorkbookTests/kvsToQueryTest.xlrdf.tar.gz"

    # clean up old session, if any
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    runner = CliRunner()

    # test list dataflows from file
    num_workbooks = len(client.list_workbooks())
    result = runner.invoke(xcalar.xc2.main.cli,
                           ["workbook", "show", "--workbook-file", wb_path])
    assert result.exit_code == 0
    assert result.output == f"1. {df_name}\n"
    assert len(client.list_workbooks()) == num_workbooks

    # xce run dataflow from file
    result_table_name = "my_final_table"
    result = runner.invoke(xcalar.xc2.main.cli, [
        "workbook", "run", "--workbook-file", wb_path,
        "--resultant-table-name", result_table_name, "--params",
        "month_val=8,aggr=count", "--sync"
    ])
    assert result.exit_code == 0
    workbook = client.get_workbook(test_wb_name)
    sess = workbook.activate()
    table = sess.get_table(table_name=result_table_name)
    record = table.get_row(0)
    assert ("Mesa Airlines" in record['Description'])
    cnt = len(list(table.records()))
    assert (cnt == 16)

    # list dafatlows by name
    result = runner.invoke(
        xcalar.xc2.main.cli,
        ["workbook", "show", "--workbook-name", workbook.name])
    assert result.exit_code == 0
    assert result.output == f"1. {df_name}\n"

    # run dataflow by name
    result_table_name = "my_final_table1"
    result = runner.invoke(xcalar.xc2.main.cli, [
        "workbook", "run", "--workbook-name", workbook.name,
        "--resultant-table-name", result_table_name, "--params",
        "month_val=8,aggr=count", "--sync"
    ])
    assert result.exit_code == 0
    table = sess.get_table(table_name=result_table_name)
    record = table.get_row(0)
    assert ("Mesa Airlines" in record['Description'])
    cnt = len(list(table.records()))
    assert (cnt == 16)


def test_run_publish_table_xc2_df(client):
    xc_api = XcalarApi(auth_instance=client.auth)
    op_api = Operators(xc_api)
    test_wb_name = "pubTableXc2Df"
    wb_path = XcalarQaDatasetPath + "/dfWorkbookTests/pubTableDfXc2.xlrdf.tar.gz"

    # clean up old session, if any
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    runner = CliRunner()

    # test publish table with xc2 workbook run
    result_table_name = "my_pub_table_xc2"
    result_table_name_upper = result_table_name.upper()
    result = runner.invoke(xcalar.xc2.main.cli, [
        "workbook", "run", "--workbook-name", test_wb_name, "--workbook-file",
        wb_path, "--dataflow-name", "test", "--resultant-table-name",
        result_table_name, "--sync"
    ])
    if result.exit_code != 0:
        print(result.exception, result.exception.args)
        assert result.exit_code == 0

    assert result.exit_code == 0, result.exception
    pub_table = op_api.listPublishedTables(result_table_name_upper).tables[0]
    assert pub_table.name == result_table_name_upper

    # test fail if exists
    result = runner.invoke(xcalar.xc2.main.cli, [
        "workbook", "run", "--workbook-name", test_wb_name, "--dataflow-name",
        "test", "--resultant-table-name", result_table_name, "--sync",
        "--fail-if-table-exists"
    ])
    assert result.exit_code != 0
    assert type(result.exception) == RuntimeError
    assert result.exception.args[
        0] == f'Table {result_table_name_upper} already exists'

    # cleanup
    op_api.unpublish(result_table_name_upper)
    client.get_workbook(test_wb_name).delete()


def test_dataflow_json_extract_update_xc2(client):
    df_path = XcalarQaDatasetPath + "/dfWorkbookTests/kvsToQueryTest.xlrdf.tar.gz"
    df_gen_path = XcalarQaDatasetPath + "/dfWorkbookTests/kvsToQueryTest_generated.xlrdf.tar.gz"
    json_path = XcalarQaDatasetPath + "/dfWorkbookTests/extract_kvsToQueryTest_tmp.json"
    # xce run dataflow from file
    runner = CliRunner()

    # extract the json
    result = runner.invoke(
        xcalar.xc2.main.cli,
        ["dataflow", "extract_kvstore", df_path, "--extract-to", json_path])
    assert result.exit_code == 0
    assert os.path.isfile(json_path)

    # update the tar without any modifications
    result = runner.invoke(xcalar.xc2.main.cli, [
        "dataflow", "update_kvstore", df_path, json_path,
        "--new-dataflow-file", df_gen_path
    ])
    assert result.exit_code == 0
    assert os.path.isfile(df_path)

    # xce run dataflow from file
    test_wb_name = "kvsToQueryTest"
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    result_table_name = "my_final_table"
    result = runner.invoke(xcalar.xc2.main.cli, [
        "workbook", "run", "--workbook-file", df_path,
        "--resultant-table-name", result_table_name, "--params",
        "month_val=8,aggr=count", "--sync", "--sched-name", "Sched0"
    ])
    assert result.exit_code == 0
    workbook = client.get_workbook(test_wb_name)
    sess = workbook.activate()
    table = sess.get_table(table_name=result_table_name)
    record1 = table.get_row(0)
    assert ("Mesa Airlines" in record1['Description'])
    cnt1 = len(list(table.records()))
    workbook.delete()

    # run updated tar.gz file
    test_wb_name = "kvsToQueryTest_generated"
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    result = runner.invoke(xcalar.xc2.main.cli, [
        "workbook", "run", "--workbook-file", df_gen_path,
        "--resultant-table-name", result_table_name, "--params",
        "month_val=8,aggr=count", "--sync", "--sched-name", "Sched1"
    ])
    assert result.exit_code == 0
    workbook = client.get_workbook(test_wb_name)
    sess = workbook.activate()
    table = sess.get_table(table_name=result_table_name)
    record2 = table.get_row(0)
    assert ("Mesa Airlines" in record2['Description'])
    cnt2 = len(list(table.records()))
    workbook.delete()

    assert cnt1 == cnt2
    assert record1 == record2


def test_xc2_command_query_stats(client):
    df_path = XcalarQaDatasetPath + "/dfWorkbookTests/kvsToQueryTest.xlrdf.tar.gz"
    # xce run dataflow from file
    runner = CliRunner()

    # xce run dataflow from file
    test_wb_name = "kvsToQueryTest"
    query_name = "qname_runkvstoquery"
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)
    result_table_name = "my_final_table"
    result = runner.invoke(xcalar.xc2.main.cli, [
        "workbook", "run", "--workbook-file", df_path, "--query-name",
        query_name, "--resultant-table-name", result_table_name, "--params",
        "month_val=8,aggr=count", "--sync", "--sched-name", "Sched2"
    ])
    print(result.exception)
    assert result.exit_code == 0
    workbook = client.get_workbook(test_wb_name)
    sess = workbook.activate()
    table = sess.get_table(table_name=result_table_name)
    record1 = table.get_row(0)
    assert ("Mesa Airlines" in record1['Description'])
    cnt1 = len(list(table.records()))

    # run stats
    tmp_file = tempfile.NamedTemporaryFile('w', encoding='utf-8')
    query_name = result.output.rstrip()
    result = runner.invoke(
        xcalar.xc2.main.cli,
        ["dataflow", "stats", query_name, "--stats-file", tmp_file.name])
    print(result.exception)
    assert result.exit_code == 0
    with open(tmp_file.name) as stats_file:
        reader = csv.DictReader(stats_file)
        a_row = next(reader)
        assert a_row["query_name"] == query_name
        assert a_row["query_status"] == "StatusOk"
        assert a_row["query_state"] == "qrFinished"
        assert a_row["completed_operations"] == "14"
        assert a_row["failed_operations"] == "0"
    workbook.delete()


@pytest.mark.parametrize("target_name,pathBuilder",
                         [(DefaultTargetName, buildNativePath)])
def test_workbook_upload_download_targets(client, target_name, pathBuilder):
    test_wb_name = "test_targets_workbook"
    wb_path = XcalarQaDatasetPath + "/dfWorkbookTests/kvsToQueryTest.xlrdf.tar.gz"
    # clean up old session, if any
    try:
        wb = client.get_workbook(test_wb_name)
        wb.delete()
    except ValueError as e:
        assert "No such workbook" in str(e)

    with open(wb_path, 'rb') as wb_file:
        wb_content = wb_file.read()
        workbook = client.upload_workbook(test_wb_name, wb_content)

    df_list_orig = workbook.list_dataflows()
    assert (len(df_list_orig) == 1)

    path = pathBuilder(
        os.path.join("downloadWbs", gethostname(), "download_workbook.tar.gz"))

    # download the workbook
    workbook.download_to_target(connector_name=target_name, path=path)

    # uploads the workbook with new name to check if it has same number of dataflows as original
    upload_wb_name = "test_upload_workbook"
    upload_wb = client.upload_workbook_from_target(
        workbook_name=upload_wb_name,
        connector_name=target_name,
        workbook_path=path)
    assert upload_wb.list_dataflows() == df_list_orig

    # cleanup
    upload_wb.delete()
    workbook.delete()
