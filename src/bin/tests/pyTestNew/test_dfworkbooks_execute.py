# Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import pytest

from xcalar.external.result_set import ResultSet

from xcalar.compute.util.Qa import XcalarQaDatasetPath


@pytest.mark.parametrize("file_path", [
    ("/dfWorkbookTests/basicTest.xlrdf.tar.gz"),
])
def test_dfworkbook(client, file_path):
    workbook_path = XcalarQaDatasetPath + file_path

    testDfWorkbookSessionOwner = "TestDfWorkbookSessionOwner"
    with open(workbook_path, 'rb') as wbFile:
        wbContent = wbFile.read()
        uploadName = testDfWorkbookSessionOwner + "-added"

        uploaded_workbook = client.add_dataflow(uploadName, wbContent)

    dfs = client.list_dataflows("*")
    ii = 0
    for df in dfs:
        print("dataflow[{}] name:{}, user:{}".format(ii, df.name, df.username))
        ii += 1

    # Create a session to run the dataflow in
    insideSessionName = testDfWorkbookSessionOwner + "-insideSession"
    inside_session = client.create_workbook(insideSessionName)
    inside_session.activate()

    # Substitution occurs on a string replacement basis where, for example,
    # using the following...all occurrences of "<name>" in the query string
    # will be replaced with "Alycia".
    params = {'name': 'Alycia', 'team': 'sharks'}
    exportTable = "myExportedTable"

    ret = client.execute_dataflow(uploadName, inside_session, params,
                                  exportTable)
    rs = ResultSet(
        client, table_name=exportTable,
        session_name=inside_session.name).record_iterator()
    recs = 0
    for r in rs:
        print("Row[{}]: {}".format(recs, r))
        recs += 1

    assert (recs == 2)

    # clean up the receiving session and the dataflow
    inside_session.delete()
    client.delete_dataflow(uploadName)


@pytest.mark.parametrize("file_path1,file_path2", [
    ("/dfWorkbookTests/cascadePart1Test.xlrdf.tar.gz",
     "/dfWorkbookTests/cascadePart2Test.xlrdf.tar.gz"),
])
def test_cascaded_dataflows(client, file_path1, file_path2):
    dataflow1_path = XcalarQaDatasetPath + file_path1
    dataflow2_path = XcalarQaDatasetPath + file_path2

    session_owner = "TestCascadedDFsOwner"
    with open(dataflow1_path, 'rb') as df1_file:
        df1_content = df1_file.read()
        df1_name = session_owner + "-added-df1"
        df1_workbook = client.add_dataflow(df1_name, df1_content)

    with open(dataflow2_path, 'rb') as df2_file:
        df2_content = df2_file.read()
        df2_name = session_owner + "-added-df2"
        df2_workbook = client.add_dataflow(df2_name, df2_content)

    # Create a session to run the dataflow in
    insideSessionName = session_owner + "-insideSession"
    inside_session = client.create_workbook(insideSessionName)
    inside_session.activate()

    exportTable = "myExportedCascadedTable"
    exportTable2 = "myExportedCascadedTable2"
    params = {'name': 'Alycia', 'city': 'Poipu', 'myTable': exportTable}

    # Run the first dataflow and export the results to a table
    ret = client.execute_dataflow(df1_name, inside_session, params,
                                  exportTable)
    rs = ResultSet(
        client, table_name=exportTable,
        session_name=inside_session.name).record_iterator()
    recs = 0
    for r in rs:
        # XXX: temp test code
        print("Row[{}]: {}".format(recs, r))
        recs += 1

    assert (recs == 2)

    # Now run the second dataflow which begins with a parameterized
    # synthesize node.  This is where "<myTable>" will be replaced
    # with the exportTable name.

    ret = client.execute_dataflow(df2_name, inside_session, params,
                                  exportTable2)
    rs = ResultSet(
        client, table_name=exportTable2,
        session_name=inside_session.name).record_iterator()
    recs = 0
    for r in rs:
        # XXX: temp test code
        print("Row[{}]: {}".format(recs, r))
        recs += 1

    assert (recs == 1)

    inside_session.delete()
    client.delete_dataflow(df1_name)
    client.delete_dataflow(df2_name)
