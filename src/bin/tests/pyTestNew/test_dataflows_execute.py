# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import pytest
import tempfile
import os
import time
import random

from xcalar.compute.util.Qa import XcalarQaDatasetPath

from xcalar.external.kvstore import KvStore
from xcalar.external.LegacyApi.XcalarApi import XcalarApiStatusException
from xcalar.external.runtime import Runtime

@pytest.mark.parametrize(
    "file_path, dataflow_name, expected_rows",
    [
    # Simple dataflows in the same workbook - first two end with linkout
    # optimized
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF1", 34),
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF2", 34),
    # this one ends with export optimized
        ("/dataflowExecuteTests/simpleTests.tar.gz", "DF3", 34),
    # Dataflow used in SDK Sprint2 demo
        ("/dataflowExecuteTests/sprint2DefOfDone.tar.gz",
         "SdkSprint2DefOfDone", 16),
    # Execute SQL node
        ("/dataflowExecuteTests/sqlMesaAirlines.tar.gz", "MesaDF", 1),
    # Workbook has multiple dataflows with depencies between the linkout
    # and linkin nodes.  Also one independent dataflow mixed in.
        ("/dataflowExecuteTests/linkOutLinkIn.tar.gz", "unrelated", 34),
    # Disabled until linkout/linkin supported in expServer
        ("/dataflowExecuteTests/linkOutLinkIn.tar.gz", "Dataflow3", 34),
    ])
def test_execute_dataflow(client, file_path, dataflow_name, expected_rows):
    workbook_path = XcalarQaDatasetPath + file_path
    test_wb_name = "TestExecuteWorkbook"
    test_final_table = "TestExecuteFinalTable"
    query_name = "TestExecuteQuery_{}".format(int(time.time()))

    with open(workbook_path, 'rb') as wb_file:
        wb_content = wb_file.read()

        workbook = client.upload_workbook(test_wb_name, wb_content)
        dataflow = workbook.get_dataflow(dataflow_name=dataflow_name)

        sess = workbook.activate()
        # Dataflows can only be executed optimized as there are no loaded
        # datasets for the unoptimized version to find.
        sched_name = random.choice(Runtime.get_dataflow_scheds())
        parallel_ops = random.choice([True, False])
        clean_job = random.choice([True, False])
        qn = sess.execute_dataflow(
            dataflow,
            optimized=True,
            table_name=test_final_table,
            query_name=query_name,
            is_async=False,
            clean_job_state=clean_job,
            sched_name=sched_name,
            parallel_operations=parallel_ops)
        table = sess.get_table(table_name=test_final_table)
        rows = len(list(table.records()))
        assert rows == expected_rows

        # Witness to SDK-355 to ensure UDF module resolution on a retina
        # created from Json works correctly.
        client._legacy_xcalar_api.setSession(sess)

        table.drop()

        # clean up
        workbook.inactivate()
        workbook.delete()


def test_execute_dataflow_export_files(client):
    workbook_path = XcalarQaDatasetPath + "/dataflowExecuteTests/simpleTests.tar.gz"
    test_wb_name = "TestMultipleExport"
    df1_name = 'DF3'
    df2_name = 'DF4'

    with open(workbook_path, 'rb') as wb_file:
        wb_content = wb_file.read()
        workbook = client.upload_workbook(test_wb_name, wb_content)
        sess = workbook.activate()

    with tempfile.TemporaryDirectory() as export_dir:
        # this dataflow contains a export node to single csv file
        df1 = workbook.get_dataflow(dataflow_name=df1_name)
        params = {'EXPORT_DIR': export_dir}
        sched_name = random.choice(Runtime.get_dataflow_scheds())
        parallel_ops = random.choice([True, False])
        clean_job = random.choice([True, False])
        sess.execute_dataflow(
            df1,
            optimized=True,
            is_async=False,
            params=params,
            clean_job_state=clean_job,
            sched_name=sched_name,
            parallel_operations=parallel_ops)

        # this dataflow has 2 export nodes to single csv files
        df2 = workbook.get_dataflow(dataflow_name=df2_name)
        sched_name = random.choice(Runtime.get_dataflow_scheds())
        parallel_ops = random.choice([True, False])
        clean_job = random.choice([True, False])
        sess.execute_dataflow(
            df2,
            optimized=True,
            is_async=False,
            params=params,
            clean_job_state=clean_job,
            sched_name=sched_name,
            parallel_operations=parallel_ops)

        # verify results
        # above dataflows execution should create three files
        files = os.listdir(export_dir)
        assert len(files) == 3
        for file_name in files:
            file_path = os.path.join(export_dir, file_name)
            with open(file_path) as fp:
                lines = fp.read().splitlines()
            assert len(lines) == 35    # 34 + 1(header)
            header = lines[0].split("\t")
            assert len(header) == 2
            print(header)

    with pytest.raises(XcalarApiStatusException) as ex:
        sched_name = random.choice(Runtime.get_dataflow_scheds())
        parallel_ops = random.choice([True, False])
        clean_job = random.choice([True, False])
        sess.execute_dataflow(
            df2,
            table_name="out_table",
            optimized=True,
            is_async=False,
            params=params,
            clean_job_state=clean_job,
            sched_name=sched_name,
            parallel_operations=parallel_ops)
        if 'Cannot have more than one export to table operation' not in str(
                ex):
            assert False

    # clean up
    workbook.inactivate()
    workbook.delete()


def test_workbook_get_dataflow(client):
    workbook_path = XcalarQaDatasetPath + "/dataflowExecuteTests/simpleTests.tar.gz"
    wb_name = "get_df_test"
    with open(workbook_path, 'rb') as wb_file:
        wb_content = wb_file.read()
        workbook = client.upload_workbook(wb_name, wb_content)

    session = workbook.activate()
    kvstore = KvStore.workbook_by_name(client, session.username, session.name)

    num_wb_dfs = 4
    assert len(kvstore.list("^DF2.*")) == len(
        workbook.list_dataflows()) == num_wb_dfs

    # add some keys with DF2 anywhere in the key
    kvstore.add_or_replace("abcDF2fhjf", "junk1", True)
    kvstore.add_or_replace("defDF2fhjf", "junk2", False)
    kvstore.add_or_replace("defDF2", "junk3", True)

    assert len(kvstore.list("DF2.*")) == num_wb_dfs + 3

    assert len(kvstore.list("^DF2.*")) == len(
        workbook.list_dataflows()) == num_wb_dfs

    df_names = ["DF1", "DF2", "DF3", "DF4"]
    assert sorted(workbook.list_dataflows()) == sorted(df_names)

    for df_name in df_names:
        df = workbook.get_dataflow(df_name)
        assert df.dataflow_name == df_name

    session.destroy()
    workbook.delete()
